#!/usr/bin/env python3

import os
import sys
import pandas as pd


def main():
    base = os.path.dirname(__file__)
    portfolio_file = os.path.join(base, 'portfolio.csv')

    # Verify the portfolio file exists
    if not os.path.exists(portfolio_file):
        print(f"Error: portfolio file not found at {portfolio_file}", file=sys.stderr)
        sys.exit(1)

    # Read the CSV into a DataFrame
    df = pd.read_csv(portfolio_file)
    if df.empty:
        print(f"Portfolio file {portfolio_file} contains no data.")
        return

    print(f"Loaded portfolio with {len(df)} rows from: {portfolio_file}")

    # Ensure card_market_value is numeric and compute total portfolio value
    if 'card_market_value' in df.columns:
        df['card_market_value'] = pd.to_numeric(df['card_market_value'], errors='coerce').fillna(0.0)
    else:
        df['card_market_value'] = 0.0

    total_portfolio_value = df['card_market_value'].sum()
    # Print simplified, formatted total
    print(f"Total Portfolio Value: ${total_portfolio_value:,.2f}")

    # Find the most valuable card (row with maximum card_market_value)
    if not df['card_market_value'].empty and df['card_market_value'].notna().any():
        try:
            idx = df['card_market_value'].idxmax()
            most_valuable_card = df.loc[idx]

            mv_name = most_valuable_card.get('card_name') if 'card_name' in most_valuable_card.index else None
            mv_id = most_valuable_card.get('card_id') if 'card_id' in most_valuable_card.index else None
            mv_value = most_valuable_card.get('card_market_value')

            mv_name = mv_name if pd.notna(mv_name) and mv_name != '' else 'UNKNOWN'
            mv_id = mv_id if pd.notna(mv_id) and mv_id != '' else 'UNKNOWN'
            mv_value = float(mv_value) if pd.notna(mv_value) else 0.0

            print('\nMost Valuable Card:')
            print(f"  Name: {mv_name}")
            print(f"  ID: {mv_id}")
            print(f"  Value: ${mv_value:,.2f}")
        except Exception as e:
            print(f"Could not determine most valuable card: {e}")
    else:
        print("No market values available to determine the most valuable card.")


if __name__ == '__main__':
    main()
