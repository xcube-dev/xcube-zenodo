# The MIT License (MIT)
# Copyright (c) 2024-2025 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
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
import tarfile
import zipfile

import fsspec
import requests
from xcube.core.store import DataStoreError, MutableDataStore
from xcube.core.store.preload import ExecutorPreloadHandle, PreloadState, PreloadStatus

from ._utils import identify_compressed_file_format, translate_data_id2uri
from .constants import (
    DOWNLOAD_FOLDER,
    PRELOAD_DECOMPRESSION_FRACTION,
    PRELOAD_DOWNLOAD_FRACTION,
    PRELOAD_PROCESSING_FRACTION,
)


class ZenodoPreloadHandle(ExecutorPreloadHandle):

    def __init__(self, cache_store: MutableDataStore, *data_ids: str, **preload_params):
        self._cache_store = cache_store
        self._cache_fs: fsspec.AbstractFileSystem = cache_store.fs
        self._cache_root = cache_store.root
        self._data_ids = data_ids
        self._io_id = f"dataset:zarr:{self._cache_store.protocol}"
        self._download_folder = self._cache_fs.sep.join(
            [self._cache_root, DOWNLOAD_FOLDER]
        )
        self._clean_up()
        super().__init__(data_ids=data_ids, **preload_params)

    def close(self) -> None:
        self._clean_up()

    def preload_data(self, data_id: str):
        self._download_data(data_id)
        self._decompress_data(data_id)
        self._prepare_data(data_id)

    def _download_data(self, data_id: str):
        # get first total size of the dataset
        uri = translate_data_id2uri(data_id)
        with requests.get(uri, stream=True) as response:
            _check_requests_response(response)
            total_size = int(response.headers.get("content-length", 0))

        # start downloading
        chunk_size = 1024 * 1024
        download_size = 0
        self.notify(
            PreloadState(
                data_id,
                status=PreloadStatus.started,
                progress=0.0,
                message="Download in progress",
            )
        )
        with requests.get(uri, stream=True) as response:
            _check_requests_response(response)
            record, filename = data_id.split("/")
            record_folder = self._cache_fs.sep.join([self._download_folder, record])
            if not self._cache_fs.isdir(record_folder):
                self._cache_fs.makedirs(record_folder)
            download_path = self._cache_fs.sep.join([record_folder, filename])
            with open(download_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    file.write(chunk)
                    download_size += len(chunk)
                    self.notify(
                        PreloadState(
                            data_id,
                            progress=PRELOAD_DOWNLOAD_FRACTION
                            * download_size
                            / total_size,
                        )
                    )

    def _decompress_data(self, data_id):
        self.notify(
            PreloadState(
                data_id,
                progress=PRELOAD_DOWNLOAD_FRACTION,
                message="Decompression in progress",
            )
        )
        record, filename = data_id.split("/")
        file_path = self._cache_fs.sep.join([self._download_folder, record, filename])
        if zipfile.is_zipfile(file_path):
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                dirname = filename.replace(".zip", "")
                extract_dir = self._cache_fs.sep.join(
                    [self._download_folder, record, dirname]
                )
                zip_ref.extractall(extract_dir)
        elif file_path.endswith(".tar") or file_path.endswith(".tar.gz"):
            format_ext = identify_compressed_file_format(file_path)
            mode = "r" if format_ext == "tar" else "r:gz"
            with tarfile.open(file_path, mode) as tar_ref:
                dirname = filename.replace(f".{format_ext}", "")
                extract_dir = self._cache_fs.sep.join(
                    [self._download_folder, record, dirname]
                )
                tar_ref.extractall(path=extract_dir, filter="data")
        self._cache_fs.delete(file_path)
        self.notify(
            PreloadState(
                data_id,
                progress=PRELOAD_DOWNLOAD_FRACTION + PRELOAD_DECOMPRESSION_FRACTION,
            )
        )

    def _prepare_data(self, data_id):
        self.notify(
            PreloadState(
                data_id,
                progress=PRELOAD_DOWNLOAD_FRACTION + PRELOAD_DECOMPRESSION_FRACTION,
                message="Write datasets to Zarr in progress",
            )
        )
        format_ext = identify_compressed_file_format(data_id)
        data_id_mod = data_id.replace(f".{format_ext}", "")
        extract_dir = self._cache_fs.sep.join([self._download_folder, data_id_mod])
        # if is_zarr_directory(extract_dir, self._cache_fs):
        #     ds = self._cache_store.open_data(extract_dir, opener_id=self._io_id)
        #     if not data_id_mod.endswith(".zarr"):
        #         data_id_mod = f"{data_id_mod}.zarr"
        #     self._cache_store.write_data(
        #         ds, data_id_mod, writer_id=self._io_id, replace=True
        #     )
        # else:
        sub_files = self._cache_fs.listdir(extract_dir)
        total_size = sum([sub_file["size"] for sub_file in sub_files])
        size_count = 0
        for sub_file in sub_files:
            sub_data_id = sub_file["name"].replace(f"{self._cache_root}/", "")
            if self._cache_store.has_data(sub_data_id):
                ds = self._cache_store.open_data(sub_data_id)
                format_ext = sub_data_id.split(".")[-1]
                sub_data_id_mod = sub_data_id.replace(f"{DOWNLOAD_FOLDER}/", "")
                if len(sub_files) == 1:
                    sub_data_id_mod = "/".join(sub_data_id_mod.split("/")[:-1])
                sub_data_id_mod = sub_data_id_mod.replace(format_ext, "zarr")
                self._cache_store.write_data(
                    ds, sub_data_id_mod, writer_id=self._io_id, replace=True
                )
            size_count += sub_file["size"]
            self.notify(
                PreloadState(
                    data_id,
                    progress=PRELOAD_DOWNLOAD_FRACTION
                    + PRELOAD_DECOMPRESSION_FRACTION
                    + (size_count / total_size) * PRELOAD_PROCESSING_FRACTION,
                )
            )
        shutil.rmtree(extract_dir)
        self.notify(PreloadState(data_id, progress=1.0, message="Preload finished"))

    def _clean_up(self) -> None:
        for data_id in self._data_ids:
            zip_filepath = self._cache_fs.sep.join([self._download_folder, data_id])
            if self._cache_fs.isfile(zip_filepath):
                self._cache_fs.delete(zip_filepath)
            format_ext = identify_compressed_file_format(data_id)
            data_id_mod = data_id.replace(f".{format_ext}", "")
            extract_dir = self._cache_fs.sep.join([self._download_folder, data_id_mod])
            if self._cache_fs.isdir(extract_dir):
                shutil.rmtree(extract_dir)


def _check_requests_response(response: requests.Response) -> None:
    if not response.ok:
        raise DataStoreError(response.raise_for_status())
