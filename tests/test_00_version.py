import cads_worker


def test_version() -> None:
    assert cads_worker.__version__ != "999"
