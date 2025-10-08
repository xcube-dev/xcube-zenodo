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

from collections.abc import Sequence
import tarfile
import rarfile
import zipfile


import fsspec
import requests
from xcube.core.store import DataStoreError, PreloadedDataStore, new_data_store
from xcube.core.store.preload import ExecutorPreloadHandle, PreloadState, PreloadStatus
from xcube.core.chunk import chunk_dataset

from ._utils import identify_compressed_file_format
from .constants import (
    TEMP_PROCESSING_FOLDER,
    PRELOAD_DECOMPRESSION_FRACTION,
    PRELOAD_DOWNLOAD_FRACTION,
    PRELOAD_PROCESSING_FRACTION,
    MAP_FILE_EXTENSION_FORMAT,
    MAP_FORMAT_FILE_EXTENSION,
    LOG,
)

_CHUNK_SIZE = 1024 * 1024


class ZenodoPreloadHandle(ExecutorPreloadHandle):

    # noinspection PyUnresolvedReferences
    def __init__(
        self, cache_store: PreloadedDataStore, *data_ids: str, **preload_params
    ):
        # setup cache store
        self._cache_store = cache_store
        self._cache_fs: fsspec.AbstractFileSystem = self._cache_store.fs
        self._cache_root = self._cache_store.root

        # setup processing store
        # noinspection PyProtectedMember
        self._process_store = new_data_store(
            "file", root=f"{TEMP_PROCESSING_FOLDER}/{self._cache_store._raw_root}"
        )
        self._process_fs: fsspec.AbstractFileSystem = self._process_store.fs
        self._process_root = self._process_store.root
        self._clean_up()
        if not self._process_fs.isdir(self._process_root):
            self._process_fs.makedirs(self._process_root)

        # trigger preload in parent class
        self._data_ids = {data_id.split("/")[-1]: data_id for data_id in data_ids}
        super().__init__(data_ids=tuple(self._data_ids.keys()), **preload_params)

    def close(self) -> None:
        self._clean_up()

    def preload_data(self, data_id: str, **preload_params):
        format_ext = identify_compressed_file_format(data_id)
        force_preload = preload_params.get("force_preload", False)
        data_id_mod = data_id.replace(f".{format_ext}", "")
        if (
            any(data_id_mod in ext_id for ext_id in self._cache_store.get_data_ids())
            and not force_preload
        ):
            self.notify(
                PreloadState(
                    data_id,
                    message="Already preloaded",
                )
            )
        else:
            self._download_data(data_id)
            self._decompress_data(data_id)
            self._prepare_data(data_id, **preload_params)

    def _download_data(self, data_id: str):
        with requests.get(self._data_ids[data_id], stream=True) as response:
            _check_requests_response(response)
            total_size = int(response.headers.get("content-length", 0))

        # start downloading
        download_size = 0
        self.notify(
            PreloadState(
                data_id,
                status=PreloadStatus.started,
                progress=0.0,
                message="Download in progress",
            )
        )
        with requests.get(self._data_ids[data_id], stream=True) as response:
            _check_requests_response(response)
            download_path = self._process_fs.sep.join([self._process_root, data_id])
            with self._process_fs.open(download_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=_CHUNK_SIZE):
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

    def _decompress_data(self, data_id: str):
        self.notify(
            PreloadState(
                data_id,
                progress=PRELOAD_DOWNLOAD_FRACTION,
                message="Decompression in progress",
            )
        )
        file_path = self._process_fs.sep.join([self._process_root, data_id])
        with self._process_fs.open(file_path, "rb") as file:

            # compressed file is a zip
            if file_path.endswith(".zip"):
                with zipfile.ZipFile(file, "r") as zip_ref:
                    dirname = data_id.replace(".zip", "")
                    extract_dir = self._process_fs.sep.join(
                        [self._process_root, dirname]
                    )
                    zip_ref.extractall(extract_dir)

            # compressed file is a tar or tar.gz
            elif file_path.endswith(".tar") or file_path.endswith(".tar.gz"):
                format_ext = identify_compressed_file_format(file_path)
                mode = "r" if format_ext == "tar" else "r:gz"
                with tarfile.open(fileobj=file, mode=mode) as tar_ref:
                    dirname = data_id.replace(f".{format_ext}", "")
                    extract_dir = self._process_fs.sep.join(
                        [self._process_root, dirname]
                    )
                    tar_ref.extractall(path=extract_dir, filter="data")

            # compressed file is a rar
            elif file_path.endswith(".rar"):
                with rarfile.RarFile(file, "r") as rar_ref:
                    rar_ref.extractall(self._process_root)

        self._process_fs.delete(file_path)

    def _prepare_data(self, data_id: str, **preload_params):
        self.notify(
            PreloadState(
                data_id,
                progress=PRELOAD_DOWNLOAD_FRACTION + PRELOAD_DECOMPRESSION_FRACTION,
                message="Prepare data",
            )
        )
        format_ext = identify_compressed_file_format(data_id)
        dirname = data_id.replace(f".{format_ext}", "")
        extract_dir = self._process_fs.sep.join([self._process_root, dirname])
        sub_files = recursive_listdir(self._process_fs, extract_dir)
        total_size = sum([sub_file["size"] for sub_file in sub_files])
        size_count = 0
        target_format = preload_params.get("target_format")
        chunks = preload_params.get("chunks")
        for sub_file in sub_files:
            source_data_id = sub_file["name"].replace(
                f"{self._process_root}{self._process_fs.sep}", ""
            )
            if self._process_store.has_data(source_data_id):
                format_ext = MAP_FILE_EXTENSION_FORMAT[source_data_id.split(".")[-1]]
                if target_format is None or target_format == format_ext:
                    self._copy_file(source_data_id, len(sub_files))
                else:
                    self._reformat_dataset(
                        source_data_id, target_format, chunks, len(sub_files)
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
        self._process_fs.rm(extract_dir, recursive=True)
        self.notify(PreloadState(data_id, progress=1.0, message="Preload finished"))

    def _copy_file(self, source_data_id: str, len_files: int) -> None:
        target_data_id = source_data_id
        if len_files == 1:
            target_data_id = self._define_single_data_id(target_data_id)

        source_fp = f"{self._process_root}{self._process_fs.sep}{source_data_id}"
        target_fp = f"{self._cache_root}{self._cache_fs.sep}{target_data_id}"
        dirname = self._cache_fs.sep.join(target_fp.split(self._cache_fs.sep)[:-1])
        if not self._cache_fs.isdir(dirname):
            self._cache_fs.makedirs(dirname)
        if self._process_fs.isdir(source_fp):
            # --- Case: Zarr or directory ---
            # Recursively copy directory contents
            for path, dirs, files in self._process_fs.walk(source_fp):
                rel_path = path.replace(source_fp, "").lstrip(self._process_fs.sep)
                target_dir = (
                    f"{target_fp}{self._cache_fs.sep}{rel_path}"
                    if rel_path
                    else target_fp
                )
                if not self._cache_fs.exists(target_dir):
                    self._cache_fs.makedirs(target_dir, exist_ok=True)

                for file in files:
                    src_file = f"{path}{self._process_fs.sep}{file}"
                    dst_file = f"{target_dir}{self._cache_fs.sep}{file}"

                    with self._process_fs.open(src_file, "rb") as src_f:
                        with self._cache_fs.open(dst_file, "wb") as dst_f:
                            while True:
                                chunk = src_f.read(_CHUNK_SIZE)
                                if not chunk:
                                    break
                                dst_f.write(chunk)
        else:
            # --- Case: Regular single file ---
            with self._process_fs.open(source_fp, "rb") as src_file:
                with self._cache_fs.open(target_fp, "wb") as dst_file:
                    while True:
                        chunk = src_file.read(_CHUNK_SIZE)
                        if not chunk:
                            break
                        dst_file.write(chunk)

    def _reformat_dataset(
        self, source_data_id: str, target_format: str, chunks: Sequence, len_files: int
    ) -> None:
        if target_format == "geotiff":
            LOG.warning("Writing data to geotiff is not supported. Zarr is used.")
            target_format = "zarr"
        target_ext = MAP_FORMAT_FILE_EXTENSION[target_format]
        format_ext = source_data_id.split(".")[-1]
        target_data_id = source_data_id.replace(format_ext, target_ext)
        if len_files == 1:
            target_data_id = self._define_single_data_id(target_data_id)
        # noinspection PyUnresolvedReferences
        opener_id = (
            f"dataset:{MAP_FILE_EXTENSION_FORMAT[format_ext]}:"
            f"{self._process_store.protocol}"
        )
        ds = self._process_store.open_data(source_data_id, opener_id=opener_id)
        ds.attrs.pop("grid_mapping", None)
        for var in ds.variables:
            ds[var].attrs.pop("grid_mapping", None)
        if chunks:
            ds = chunk_dataset(
                ds,
                {dim: chunk for (dim, chunk) in zip(ds.dims, chunks)},
                format_name=target_format,
            )

        # noinspection PyUnresolvedReferences
        writer_id = f"dataset:{target_format}:{self._cache_store.protocol}"
        # noinspection PyUnresolvedReferences
        self._cache_store.write_data(
            ds, target_data_id, writer_id=writer_id, replace=True
        )

    def _define_single_data_id(self, data_id: str) -> str:
        dirname = data_id.split(self._cache_fs.sep, maxsplit=1)[0]
        file_ext = data_id.split(".")[-1]
        if dirname.endswith(file_ext):
            return dirname
        else:
            return f"{dirname}.{file_ext}"

    def _clean_up(self) -> None:
        if self._process_fs.isdir(self._process_root):
            self._process_fs.rm(self._process_root, recursive=True)


def _check_requests_response(response: requests.Response) -> None:
    if not response.ok:
        raise DataStoreError(str(response.raise_for_status()))


def recursive_listdir(fs: fsspec.AbstractFileSystem, path: str) -> list:
    items = fs.listdir(path)
    files = []

    for item in items:
        if item["type"] == "directory":
            if fs.exists(fs.sep.join([item["name"], ".zattrs"])):
                files.append(item)
            else:
                files.extend(recursive_listdir(fs, item["name"]))
        else:
            files.append(item)

    return files
