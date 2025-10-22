#!/usr/bin/env python3


import sys
import os

try:
    # import the update_portfolio module (it lives in the same directory)
    import update_portfolio
except Exception:
    update_portfolio = None
try:
    import generate_summary
except Exception:
    generate_summary = None


def main():
    base = os.path.dirname(__file__)
    print(f"Starting pipeline in {base}", file=sys.stderr)

    # ETL update step
    print("Starting ETL: update_portfolio", file=sys.stderr)
    if update_portfolio is None:
        print("Could not import update_portfolio module.", file=sys.stderr)
        sys.exit(1)

    try:
        update_portfolio.main()
        print("ETL update_portfolio completed successfully.", file=sys.stderr)
    except Exception as e:
        print(f"ETL update failed: {e}", file=sys.stderr)
        sys.exit(1)

    # Reporting step
    print("Starting reporting: generate_summary", file=sys.stderr)
    if generate_summary is None:
        print("Could not import generate_summary module.", file=sys.stderr)
        sys.exit(1)

    try:
        generate_summary.main()
        print("Reporting completed successfully.", file=sys.stderr)
    except Exception as e:
        print(f"Reporting failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
