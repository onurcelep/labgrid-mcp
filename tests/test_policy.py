from labgrid_mcp.config import load_config
from labgrid_mcp.policy import Category, annotations, is_enabled


def test_default_enables_everything_except_flash_and_place_delete() -> None:
    cfg = load_config(env={})
    for cat in Category:
        expected = cat not in (Category.FLASH, Category.PLACE_DELETE)
        assert is_enabled(cat, cfg) is expected, cat


def test_flash_requires_explicit_allow() -> None:
    cfg = load_config(env={"LABGRID_MCP_ALLOW": "flash"})
    assert is_enabled(Category.FLASH, cfg) is True


def test_place_delete_requires_explicit_allow() -> None:
    cfg = load_config(env={"LABGRID_MCP_ALLOW": "place_delete"})
    assert is_enabled(Category.PLACE_DELETE, cfg) is True


def test_allowlist_restricts_others() -> None:
    cfg = load_config(env={"LABGRID_MCP_ALLOW": "power"})
    assert is_enabled(Category.POWER, cfg) is True
    assert is_enabled(Category.IO, cfg) is False
    assert is_enabled(Category.CONSOLE, cfg) is False


def test_allow_metadata_env_selects_only_metadata() -> None:
    cfg = load_config(env={"LABGRID_MCP_ALLOW": "metadata"})
    assert is_enabled(Category.METADATA, cfg) is True
    assert is_enabled(Category.POWER, cfg) is False
    assert is_enabled(Category.CONSOLE, cfg) is False
    assert is_enabled(Category.PLACE_DELETE, cfg) is False


def test_ssh_default_on() -> None:
    # Category.SSH is default-on (remote command exec is the acquired-place
    # operating model, same class as power/console), NOT explicit-opt-in.
    assert is_enabled(Category.SSH, load_config(env={})) is True


def test_allow_ssh_env_selects_only_ssh() -> None:
    cfg = load_config(env={"LABGRID_MCP_ALLOW": "ssh"})
    assert is_enabled(Category.SSH, cfg) is True
    assert is_enabled(Category.POWER, cfg) is False
    assert is_enabled(Category.CONSOLE, cfg) is False


def test_readonly_disables_ssh() -> None:
    cfg = load_config(env={"LABGRID_MCP_READONLY": "1", "LABGRID_MCP_ALLOW": "ssh"})
    assert is_enabled(Category.SSH, cfg) is False


def test_reads_always_enabled() -> None:
    for env in ({}, {"LABGRID_MCP_READONLY": "1"}, {"LABGRID_MCP_ALLOW": ""}):
        assert is_enabled(Category.READ, load_config(env=env)) is True


def test_readonly_disables_all_mutations() -> None:
    cfg = load_config(env={"LABGRID_MCP_READONLY": "1", "LABGRID_MCP_ALLOW": "flash,power"})
    for cat in Category:
        if cat is not Category.READ:
            assert is_enabled(cat, cfg) is False, cat


def test_annotations_shape() -> None:
    assert annotations(read_only=True) == {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": False,
    }
    assert annotations(destructive=True, idempotent=True) == {
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": True,
    }
