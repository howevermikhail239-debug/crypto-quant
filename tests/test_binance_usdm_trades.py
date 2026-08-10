from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from crypto_quant.ingestion.binance.usdm_trades import CONTRACT_ID, FIELDS, identity, normalize


@pytest.mark.parametrize(("maker", "side", "sign"), [("true", "SELL", -1), ("false", "BUY", 1)])
def test_usdm_six_field_contract_and_maker_truth(maker, side, sign):
    row = dict(zip(FIELDS, ("1", "100", "2", "200", "1782864000000", maker), strict=True))
    value = normalize(
        row,
        ordinal=0,
        ident=identity("BTCUSDT"),
        day=date(2026, 7, 1),
        raw_ref="x",
        raw_hash="a" * 64,
        retrieved_at=datetime.now(UTC),
    )
    assert value["taker_side"] == side and value["signed_quantity"] == Decimal(2) * sign
    assert value["source_timestamp_unit"] == "ms" and value["data_contract_version"] == CONTRACT_ID
    assert value["is_rpi_trade"] is None


def test_usdm_identity_and_strict_route():
    eth = identity("ETHUSDT")
    assert (
        eth.market_type == "perpetual"
        and eth.contract_type == "linear_perpetual"
        and eth.settle_asset == "USDT"
    )
    with pytest.raises(ValueError):
        identity("SOLUSDT")
