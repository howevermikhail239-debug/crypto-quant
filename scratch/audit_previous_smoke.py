"""Audit Revision Script for Previous Smoke Gap gap_11588b0a09dc43ff."""

from pathlib import Path

from crypto_quant.ingestion.realtime_recovery import audit_revision_previous_smoke_gap


def main():
    root = Path("C:/crypto_quant_data")
    revised = audit_revision_previous_smoke_gap(root, "gap_11588b0a09dc43ff")
    if revised:
        print("Successfully created audit revision for gap_11588b0a09dc43ff:")
        print(f"  New Status: {revised.status.value}")
        print(f"  Coverage Proven: {revised.coverage_proven}")
        print(f"  Notes: {revised.notes}")
    else:
        print("Target gap_11588b0a09dc43ff not found in registry manifest.")

if __name__ == "__main__":
    main()
