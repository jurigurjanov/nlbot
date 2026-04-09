from pathlib import Path


# exec() populates this module's namespace with cli.py definitions so that
# test monkeypatching via ``monkeypatch.setattr(run_bot, "name", ...)`` affects
# the actual call sites inside main().  Path is always relative to __file__,
# never user-controlled.
_CLI_SOURCE_PATH = Path(__file__).resolve().parent / "xtb_bot" / "cli.py"
if not _CLI_SOURCE_PATH.is_file():
    raise RuntimeError(f"CLI source not found: {_CLI_SOURCE_PATH}")
exec(compile(_CLI_SOURCE_PATH.read_text(encoding="utf-8"), str(_CLI_SOURCE_PATH), "exec"), globals())
