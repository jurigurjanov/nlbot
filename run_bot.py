from pathlib import Path


# Keep the script entrypoint monkeypatch-friendly for tests while sourcing the
# implementation from the single canonical CLI module.
_CLI_SOURCE_PATH = Path(__file__).resolve().parent / "xtb_bot" / "cli.py"
exec(compile(_CLI_SOURCE_PATH.read_text(encoding="utf-8"), str(_CLI_SOURCE_PATH), "exec"), globals())
