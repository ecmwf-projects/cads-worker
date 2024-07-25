import contextlib
import datetime
from pathlib import Path
from typing import Any

import cacholote
import pytest

from cads_worker import entry_points

does_not_raise = contextlib.nullcontext


@cacholote.cacheable
def cached_now() -> datetime.datetime:
    return datetime.datetime.now()


@pytest.mark.parametrize(
    "collection_id,all_collections,raises",
    [
        (["foo"], False, does_not_raise()),
        ([], True, does_not_raise()),
        ([], False, pytest.raises(ValueError)),
        (["foo"], True, pytest.raises(ValueError)),
    ],
)
def test_cache_entries(
    tmp_path: Path,
    collection_id: list[str],
    all_collections: bool,
    raises: contextlib.nullcontext[Any],
) -> None:
    today = datetime.datetime.now(tz=datetime.timezone.utc)
    tomorrow = today + datetime.timedelta(days=1)
    yeasterday = today - datetime.timedelta(days=1)

    with cacholote.config.set(
        cache_db_urlpath=f"sqlite:///{tmp_path / 'cacholote.db'}",
        tag="foo",
    ):
        now = cached_now()
        assert now == cached_now()

        with raises:
            count = entry_points._expire_cache_entries(
                before=tomorrow,
                after=yeasterday,
                collection_id=collection_id,
                all_collections=all_collections,
            )
            assert count == 1
            assert now != cached_now()
