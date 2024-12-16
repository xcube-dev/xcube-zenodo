# The MIT License (MIT)
# Copyright (c) 2024 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import shutil
import threading
import time
from typing import Callable
import tarfile
import zipfile

import fsspec
import numpy as np
import tabulate
import requests
import xarray as xr
from xcube.core.store import DataStoreError
from xcube.core.store import MutableDataStore

from ._utils import identify_file_format
from ._utils import translate_data_id2uri
from .constants import LOG


class Event:

    def __init__(self, data_id: str, total_size: int | float):
        self.data_id = data_id
        self.status = "Not started"
        self.progress = 0.0
        self.message = "Preloading not started yet."
        self.total_size = total_size
        self._callback = None

    def subscribe(self, callback: Callable[[], None]):
        self._callback = callback

    def notify(self):
        if self._callback is not None:
            self._callback()

    def update(self, status: str, progress: float, message: str):
        self.status = status
        self.progress = progress
        self.message = message
        self.notify()


class PreloadHandle:

    def __init__(self, cache_store: MutableDataStore, *data_ids: str, **preload_params):
        self._is_cancelled = False
        self._is_closed = False
        self._cache_store = cache_store
        self._cache_fs: fsspec.AbstractFileSystem = cache_store.fs
        self._cache_root = cache_store.root
        self._data_ids = data_ids
        self._preload_params = preload_params
        self._download_folder_name = "downloads"
        self._download_folder = self._cache_fs.sep.join(
            [self._cache_root, self._download_folder_name]
        )
        self._events = [Event(data_id, np.nan) for data_id in data_ids]
        self.lock = threading.Lock()
        if preload_params.get("monitor_preload"):
            for event in self._events:
                event.subscribe(self._monitor_preload)
        self._thread_download = None
        self._thread_decompress = None
        self._thread_prepare = None

    @property
    def is_cancelled(self) -> bool:
        return self._is_cancelled

    def cancel(self):
        self._is_cancelled = True
        self._thread_prepare.join()
        self._thread_decompress.join()
        self._thread_download.join()
        for event in self._events:
            event.update("Canceled", np.nan, "Preload has been canceled by user.")
        self.close()

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    def close(self):
        self._is_closed = True
        for event in self._events:
            if event.status != "Preloaded":
                if self._cache_store.has_data(event.data_id):
                    self._cache_store.delete_data(event.data_id)
                else:
                    record, filename = event.data_id.split("/")
                    format_id = identify_file_format(event.data_id)
                    dirname = filename.replace(f".{format_id}", "")
                    data_id_mod = f"{record}/{dirname}"
                    list_data_ids = self._cache_store.list_data_ids()
                    list_data_ids_mod = [
                        data_id for data_id in list_data_ids if data_id_mod in data_id
                    ]
                    for data_id in list_data_ids_mod:
                        self._cache_store.delete_data(data_id)
        if self._cache_fs.isdir(self._download_folder):
            shutil.rmtree(self._download_folder)

    def _monitor_preload(self):
        rows = [
            [
                event.data_id,
                event.status,
                f"{event.progress * 100:.2f}%",
                event.message,
            ]
            for event in self._events
        ]
        if is_jupyter():
            import IPython.display

            table = tabulate.tabulate(
                rows,
                headers=["Data ID", "Status", "Progress", "Message"],
                tablefmt="html",
            )
            IPython.display.clear_output(wait=True)
            IPython.display.display(table)

    def preload_data(self, *data_ids: str, **preload_params):
        self._download_data(*data_ids)
        self._decompress_data(*data_ids)
        self._prepare_data(*data_ids, **preload_params)

    def _download_data(self, *data_ids: str):
        # get first total size of all datasets and initialize it as Events
        for i, data_id in enumerate(data_ids):
            uri = translate_data_id2uri(data_id)
            with requests.get(uri, stream=True) as response:
                if not response.ok:
                    raise DataStoreError(response.raise_for_status())
                self._events[i].total_size = int(
                    response.headers.get("content-length", 0)
                )

        # start downloading
        chunk_size = 1024 * 1024

        def download():
            for i, data_id in enumerate(data_ids):
                download_size = 0
                self._events[i].update("Download started", 0, "")
                uri = translate_data_id2uri(data_id)
                with requests.get(uri, stream=True) as response:
                    if not response.ok:
                        raise DataStoreError(response.raise_for_status())
                    record, filename = data_id.split("/")
                    record_folder = self._cache_fs.sep.join(
                        [self._download_folder, record]
                    )
                    if not self._cache_fs.isdir(record_folder):
                        self._cache_fs.makedirs(record_folder)
                    download_path = self._cache_fs.sep.join([record_folder, filename])
                    with open(download_path, "wb") as file:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            file.write(chunk)
                            download_size += len(chunk)
                            self._events[i].update(
                                "Download started",
                                download_size / self._events[i].total_size,
                                "",
                            )
                            if self._is_cancelled:
                                break
                self._events[i].update("Downloaded", 1.0, "")

        self._thread_download = threading.Thread(
            target=download, daemon=True, name="download_data"
        )
        self._thread_download.start()

    def _decompress_data(self, *data_ids):
        def decompress():
            for i, data_id in enumerate(data_ids):
                while not self._events[i].status == "Downloaded":
                    time.sleep(1)
                self._events[i].update("Decompression started", np.nan, "")
                record, filename = data_id.split("/")
                file_path = self._cache_fs.sep.join(
                    [self._download_folder, record, filename]
                )
                if zipfile.is_zipfile(file_path):
                    with zipfile.ZipFile(file_path, "r") as zip_ref:
                        dirname = filename.replace(".zip", "")
                        extract_dir = self._cache_fs.sep.join(
                            [self._download_folder, record, dirname]
                        )
                        zip_ref.extractall(extract_dir)
                elif file_path.endswith(".tar"):
                    with tarfile.open(file_path, "r") as tar_ref:
                        dirname = filename.replace(".tar", "")
                        extract_dir = self._cache_fs.sep.join(
                            [self._download_folder, record, dirname]
                        )
                        tar_ref.extractall(path=extract_dir)
                elif file_path.endswith(".tar.gz"):
                    with tarfile.open(file_path, "r:gz") as tar_ref:
                        dirname = filename.replace(".tar.gz", "")
                        extract_dir = self._cache_fs.sep(
                            [self._download_folder, record, dirname]
                        )
                        tar_ref.extractall(path=extract_dir)
                self._events[i].update("Decompressed", np.nan, "")
                if self._is_cancelled:
                    break

        self._thread_decompress = threading.Thread(
            target=decompress, daemon=True, name="decompress_data"
        )
        self._thread_decompress.start()

    def _prepare_data(self, *data_ids, **preload_params):
        def prepare():
            for i, data_id in enumerate(data_ids):
                while not self._events[i].status == "Decompressed":
                    time.sleep(1)
                self._events[i].update("File processing started", np.nan, "")
                record, filename = data_id.split("/")
                format_id = identify_file_format(data_id)
                filename_unzip = filename.replace(f".{format_id}", "")
                extract_dir = self._cache_fs.sep.join(
                    [self._download_folder, record, filename_unzip]
                )
                dss = []
                sub_files = self._cache_fs.listdir(extract_dir)
                for sub_file in sub_files:
                    sub_data_id = sub_file["name"].replace(f"{self._cache_root}/", "")
                    if not self._cache_store.has_data(sub_data_id):
                        LOG.debug(
                            f"File with data ID {sub_data_id} cannot be opened, "
                            f"and thus will not be considered."
                        )
                    dss.append(self._cache_store.open_data(sub_data_id))
                if len(dss) == 1:
                    self._cache_store.write_data(
                        dss[0],
                        f"{record}/{filename_unzip}",
                        writer_id="dataset:zarr:file",
                    )
                elif preload_params.get("merge"):
                    ds = xr.merge(dss)
                    self._cache_store.write_data(
                        ds, data_id, writer_id="dataset:zarr:file"
                    )
                else:
                    for ds, sub_file in zip(dss, sub_files):
                        sub_fname = sub_file["name"].split("/")[-1]
                        data_id = (
                            f"{record}/{filename_unzip}/"
                            f"{".".join(sub_fname.split(".")[:-1])}.zarr"
                        )
                        self._cache_store.write_data(
                            ds, data_id, writer_id="dataset:zarr:file"
                        )
                        LOG.info(
                            f"Merge is set to False. The sub-dataset is "
                            f"written to {data_id}"
                        )
                self._events[i].update("Preloaded", np.nan, "")
                if self._is_cancelled:
                    break
            self.close()

        self._thread_prepare = threading.Thread(
            target=prepare, daemon=True, name="prepare_data"
        )
        self._thread_prepare.start()


def is_jupyter():
    import IPython

    return "ZMQInteractiveShell" in IPython.get_ipython().__class__.__name__
