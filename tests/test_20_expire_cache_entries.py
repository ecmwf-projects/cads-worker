import datetime
import pathlib

import cacholote
import pytest

from cads_worker import entry_points

TODAY = datetime.datetime.now(tz=datetime.timezone.utc)
TOMORROW = TODAY + datetime.timedelta(days=1)
YESTERDAY = TODAY - datetime.timedelta(days=1)


@cacholote.cacheable
def cached_now() -> datetime.datetime:
    return datetime.datetime.now()


@pytest.mark.parametrize(
    "collection_id,before,after",
    [
        (["foo"], None, None),
        (None, TOMORROW, None),
        (None, None, YESTERDAY),
        (["foo"], TOMORROW, YESTERDAY),
    ],
)
def test_cache_entries(
    tmp_path: pathlib.Path,
    collection_id: list[str] | None,
    before: datetime.datetime | None,
    after: datetime.datetime | None,
) -> None:
    with cacholote.config.set(
        cache_db_urlpath=f"sqlite:///{tmp_path / 'cacholote.db'}",
        tag="foo",
    ):
        now = cached_now()
        assert now == cached_now()

        count = entry_points._expire_cache_entries(
            collection_id=collection_id,
            before=before,
            after=after,
        )
        assert count == 1
        assert now != cached_now()
