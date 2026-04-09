from __future__ import annotations

from xtb_bot.config import _resolve, _resolve_dual_env


def test_resolve_uses_file_value_when_env_is_missing(monkeypatch):
    monkeypatch.delenv("XTB_TEST_CONFIG_RESOLVE", raising=False)
    assert _resolve({"sample_key": "from_file"}, "XTB_TEST_CONFIG_RESOLVE", "sample_key", "fallback") == "from_file"


def test_resolve_prefers_env_over_file(monkeypatch):
    monkeypatch.setenv("XTB_TEST_CONFIG_RESOLVE", "from_env")
    assert _resolve({"sample_key": "from_file"}, "XTB_TEST_CONFIG_RESOLVE", "sample_key", "fallback") == "from_env"


def test_resolve_dual_env_uses_file_value_when_envs_missing(monkeypatch):
    monkeypatch.delenv("XTB_TEST_CONFIG_PRIMARY", raising=False)
    monkeypatch.delenv("XTB_TEST_CONFIG_SECONDARY", raising=False)
    assert (
        _resolve_dual_env(
            {"sample_key": 0},
            "XTB_TEST_CONFIG_PRIMARY",
            "XTB_TEST_CONFIG_SECONDARY",
            "sample_key",
            10,
        )
        == 0
    )

