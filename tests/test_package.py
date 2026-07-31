import labgrid_mcp


def test_version() -> None:
    assert labgrid_mcp.__version__ == "0.1.0"


def test_labgrid_importable() -> None:
    import labgrid.remote.common  # noqa: F401
    import labgrid.remote.generated  # noqa: F401
