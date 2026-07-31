import pytest

from labgrid_mcp.config import Config, load_config  # noqa: F401


def test_defaults() -> None:
    cfg = load_config(env={})
    assert cfg.coordinator == "127.0.0.1:20408"
    assert cfg.hostname  # falls back to real hostname
    assert cfg.username  # falls back to real user
    assert cfg.readonly is False
    assert cfg.allow is None
    assert cfg.acquire_timeout == 120.0
    assert cfg.ssh_keyfile is None


def test_coordinator_bare_host_gets_default_port() -> None:
    cfg = load_config(env={"LG_COORDINATOR": "lab.example.com"})
    assert cfg.coordinator == "lab.example.com:20408"


def test_coordinator_with_port_kept() -> None:
    cfg = load_config(env={"LG_COORDINATOR": "lab.example.com:1234"})
    assert cfg.coordinator == "lab.example.com:1234"


def test_identity_from_env() -> None:
    cfg = load_config(env={"LG_HOSTNAME": "myhost", "LG_USERNAME": "me"})
    assert cfg.identity == "myhost/me"


@pytest.mark.parametrize("raw", ["1", "true", "yes"])
def test_readonly_truthy(raw: str) -> None:
    assert load_config(env={"LABGRID_MCP_READONLY": raw}).readonly is True


def test_readonly_falsy() -> None:
    assert load_config(env={"LABGRID_MCP_READONLY": "0"}).readonly is False


def test_allow_parsing_normalizes() -> None:
    cfg = load_config(env={"LABGRID_MCP_ALLOW": " Power, flash ,io"})
    assert cfg.allow == frozenset({"power", "flash", "io"})


def test_allow_empty_string_is_empty_set() -> None:
    cfg = load_config(env={"LABGRID_MCP_ALLOW": ""})
    assert cfg.allow == frozenset()


def test_acquire_timeout_parsed() -> None:
    cfg = load_config(env={"LABGRID_MCP_ACQUIRE_TIMEOUT": "30.5"})
    assert cfg.acquire_timeout == 30.5


def test_ssh_keyfile_from_env() -> None:
    cfg = load_config(env={"LABGRID_MCP_SSH_KEYFILE": "/home/me/.ssh/id_ed25519"})
    assert cfg.ssh_keyfile == "/home/me/.ssh/id_ed25519"


def test_ssh_keyfile_unset_is_none() -> None:
    assert load_config(env={}).ssh_keyfile is None


def test_ssh_keyfile_empty_string_is_none() -> None:
    assert load_config(env={"LABGRID_MCP_SSH_KEYFILE": ""}).ssh_keyfile is None


def test_config_is_frozen() -> None:
    cfg = load_config(env={})
    with pytest.raises(AttributeError):
        cfg.readonly = True  # type: ignore[misc]
