import fsspec.implementations.local
from fsspec.utils import stringify_path


class CCIFileSystem(fsspec.implementations.local.LocalFileSystem):
    protocol = "cci"

    @classmethod
    def _strip_protocol(cls, path):
        assert isinstance(cls.protocol, str)
        path = stringify_path(path)
        if path.startswith(f"{cls.protocol}:"):
            path = path.replace(cls.protocol, "file", 1)
        return super()._strip_protocol(path)

    def unstrip_protocol(self, name):
        assert isinstance(self.protocol, str)
        name = self._strip_protocol(name)
        return f"{self.protocol}://{name}"


class CCI1FileSystem(CCIFileSystem):
    protocol = "cci1"


class CCI2FileSystem(CCIFileSystem):
    protocol = "cci2"
