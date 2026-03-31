import os

import fsspec
import pytest


@pytest.mark.parametrize("protocol", ["cci1", "cci2"])
def test_unstrip_protocol(protocol: str) -> None:
    fs = fsspec.filesystem(protocol)
    assert fs.unstrip_protocol(".") == f"{protocol}://{os.getcwd()}"
