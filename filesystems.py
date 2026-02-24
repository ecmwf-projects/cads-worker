import fsspec.implementations.local


class CCI1FileSystem(fsspec.implementations.local.LocalFileSystem):
    protocol = "cci1"


class CCI2FileSystem(fsspec.implementations.local.LocalFileSystem):
    protocol = "cci2"
