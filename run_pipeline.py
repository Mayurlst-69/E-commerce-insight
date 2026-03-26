"""
run_pipeline.py — One-Click Entry Point
=========================================
This is the only file you need to run. It orchestrates:
  1. Ingestion   (src/ingest.py)
  2. Transform   (src/transform.py)
  3. Dashboard   (src/dashboard.py via streamlit)

Usage:
  python run_pipeline.py              # full pipeline + launch dashboard
  python run_pipeline.py --skip-etl  # skip ingest/transform, just launch dashboard
  python run_pipeline.py --etl-only  # run pipeline only, don't launch dashboard
"""

import sys
import os
import subprocess

# Add src to path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


def print_banner():
    print("\n" + "=" * 55)
    print("  RETAIL SALES INTELLIGENCE — PIPELINE RUNNER")
    print("=" * 55 + "\n")


def run_etl():
    """Run ingestion then transform."""
    from ingest import run_ingestion, scrape_exchange_rate, save_exchange_rate
    from transform import run_transform

    # Step 1
    success = run_ingestion(auto_download=False)
    if not success:
        print("❌  Ingestion failed. Aborting.\n")
        return False

    rate = scrape_exchange_rate()
    save_exchange_rate(rate)

    # Step 2
    success = run_transform()
    if not success:
        print("❌  Transform failed. Aborting.\n")
        return False

    return True


def launch_dashboard():
    """Launch Streamlit dashboard in the browser."""
    print("=" * 55)
    print("  STEP 3 — LAUNCHING DASHBOARD")
    print("=" * 55)
    print("\n  Opening: http://localhost:8501")
    print("  Press Ctrl+C to stop.\n")

    dashboard_path = os.path.join(os.path.dirname(__file__), "src", "dashboard.py")
    subprocess.run(["streamlit", "run", dashboard_path], check=True)


if __name__ == "__main__":
    print_banner()

    skip_etl  = "--skip-etl"  in sys.argv
    etl_only  = "--etl-only"  in sys.argv

    if not skip_etl:
        ok = run_etl()
        if not ok:
            sys.exit(1)
    else:
        print("⏭️  Skipping ETL (--skip-etl flag set)\n")

    if not etl_only:
        launch_dashboard()
    else:
        print("✅  ETL complete. Dashboard not launched (--etl-only flag set).\n")
