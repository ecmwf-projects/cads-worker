from typing import Any


def submit_workflow(
    setup_code: str,
    entry_point: str,
    kwargs: dict[str, Any] = {},
    metadata: dict[str, Any] = {},
) -> dict[str, Any] | list[dict[str, Any]]:
    import cacholote

    exec(setup_code, globals())
    results = eval(f"{entry_point}(metadata=metadata, **kwargs)")
    return cacholote.encode.dumps(results)
