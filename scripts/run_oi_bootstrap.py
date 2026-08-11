"""Full production bootstrap and audit script for Phase 1D.2 Open Interest."""
import json
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from crypto_quant.ingestion.binance.open_interest import ingest_binance_open_interest
from crypto_quant.ingestion.bybit.open_interest import ingest_bybit_open_interest

root = Path("C:/crypto_quant_data")

print("=" * 60)
print("PHASE 1D.2: LIVE OPEN INTEREST BOOTSTRAP (5m)")
print("=" * 60)

# 1. Binance USD-M
print("\n[1/4] Ingesting Binance USD-M BTCUSDT 5m...")
res_bin_btc = ingest_binance_open_interest("BTCUSDT", root, period="5m")
print(f"  Records: {res_bin_btc['records_count']}, Coverage: {res_bin_btc['observed_source_coverage_start']} -> {res_bin_btc['observed_source_coverage_end']}")

print("\n[2/4] Ingesting Binance USD-M ETHUSDT 5m...")
res_bin_eth = ingest_binance_open_interest("ETHUSDT", root, period="5m")
print(f"  Records: {res_bin_eth['records_count']}, Coverage: {res_bin_eth['observed_source_coverage_start']} -> {res_bin_eth['observed_source_coverage_end']}")

# 2. Bybit Linear
print("\n[3/4] Ingesting Bybit Linear BTCUSDT 5m...")
res_byb_btc = ingest_bybit_open_interest("BTCUSDT", root, period="5m")
print(f"  Records: {res_byb_btc['records_count']}, Coverage: {res_byb_btc['observed_source_coverage_start']} -> {res_byb_btc['observed_source_coverage_end']}")

print("\n[4/4] Ingesting Bybit Linear ETHUSDT 5m...")
res_byb_eth = ingest_bybit_open_interest("ETHUSDT", root, period="5m")
print(f"  Records: {res_byb_eth['records_count']}, Coverage: {res_byb_eth['observed_source_coverage_start']} -> {res_byb_eth['observed_source_coverage_end']}")

# 3. Comprehensive Verification & Stats
print("\n" + "=" * 60)
print("AUDIT & VERIFICATION OF PERSISTED OPEN INTEREST DATASETS")
print("=" * 60)

summary_stats = []

for ex in ["binance", "bybit"]:
    for sym in ["BTCUSDT", "ETHUSDT"]:
        pdir = root / "normalized" / "open_interest" / "v1" / f"exchange={ex}" / "market_type=perpetual" / f"symbol={sym}" / "period=5m"
        pfiles = sorted(pdir.rglob("*.parquet"))
        if not pfiles:
            print(f"ERROR: No parquet files found for {ex} {sym} in {pdir}")
            continue
        total_size = sum(f.stat().st_size for f in pfiles)
        tbl = pq.ParquetFile(pfiles[0]).read()
        oi_bases = [Decimal(x) for x in tbl["oi_base"].to_pylist()]
        obs_times = tbl["observation_time"].to_pylist()

        notionals = [Decimal(x) for x in tbl["oi_notional"].to_pylist() if x is not None] if "oi_notional" in tbl.schema.names and tbl["oi_notional"][0].as_py() is not None else []
        singles = [Decimal(x) for x in tbl["single_side_oi_base"].to_pylist() if x is not None] if "single_side_oi_base" in tbl.schema.names and tbl["single_side_oi_base"][0].as_py() is not None else []

        stat = {
            "exchange": ex,
            "symbol": sym,
            "period": "5m",
            "parquet_files": len(pfiles),
            "size_kb": round(total_size / 1024, 2),
            "row_count": len(obs_times),
            "start": obs_times[0].isoformat(),
            "end": obs_times[-1].isoformat(),
            "min_oi_base": f"{min(oi_bases):.4f}",
            "max_oi_base": f"{max(oi_bases):.4f}",
            "avg_oi_base": f"{sum(oi_bases)/len(oi_bases):.4f}",
            "notional_available": len(notionals) > 0,
            "min_notional": f"${min(notionals):,.2f}" if notionals else "N/A",
            "max_notional": f"${max(notionals):,.2f}" if notionals else "N/A",
            "single_side_available": len(singles) > 0,
            "min_single_oi": f"{min(singles):.4f}" if singles else "N/A",
            "max_single_oi": f"{max(singles):.4f}" if singles else "N/A",
        }
        summary_stats.append(stat)

        print(f"\n--- {ex.upper()} {sym} 5m ---")
        print(f"  Parquet: {pfiles[0].name} ({stat['size_kb']} KB, {stat['row_count']} rows)")
        print(f"  Coverage: {stat['start']} -> {stat['end']}")
        print(f"  OI Base: min={stat['min_oi_base']}, max={stat['max_oi_base']}, avg={stat['avg_oi_base']}")
        if stat["notional_available"]:
            print(f"  OI Notional: min={stat['min_notional']}, max={stat['max_notional']}")
        if stat["single_side_available"]:
            print(f"  Single-Side OI Base: min={stat['min_single_oi']}, max={stat['max_single_oi']}")

# 4. Save JSON summary for report generation
summary_path = Path("docs/phase1d2_oi_stats.json")
summary_path.parent.mkdir(parents=True, exist_ok=True)
summary_path.write_text(json.dumps(summary_stats, indent=2), encoding="utf-8")
print(f"\nSaved stats summary to {summary_path}")
