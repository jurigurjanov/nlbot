from __future__ import annotations

from xtb_bot.models import Position, Side
from xtb_bot.position_book import PositionBook


def _make_position(
    position_id: str,
    *,
    symbol: str = "DE40",
    status: str = "open",
    opened_at: float = 1_700_000_000.0,
) -> Position:
    return Position(
        position_id=position_id,
        symbol=symbol,
        side=Side.BUY,
        volume=1.0,
        open_price=20_000.0,
        stop_loss=19_900.0,
        take_profit=20_150.0,
        opened_at=opened_at,
        status=status,
    )


def test_position_book_bootstrap_keeps_closing_positions() -> None:
    book = PositionBook()

    book.bootstrap(
        [
            _make_position("open-1", status="open"),
            _make_position("closing-1", status="closing", opened_at=1_700_000_010.0),
            _make_position("closed-1", status="closed", opened_at=1_700_000_020.0),
        ]
    )

    assert book.count() == 2
    assert book.get_by_id("open-1") is not None
    assert book.get_by_id("closing-1") is not None
    assert book.get_by_id("closed-1") is None


def test_position_book_upsert_treats_closing_as_active() -> None:
    book = PositionBook()

    book.upsert(_make_position("deal-1", status="closing"))
    active = book.get_by_id("deal-1")
    assert active is not None
    assert active.status == "closing"

    book.upsert(_make_position("deal-1", status="closed"))

    assert book.get_by_id("deal-1") is None
    assert book.get("DE40") is None
