#!/usr/bin/env python3


import os
import sys
import pandas as pd

from _load_lookup_data import load_lookup_dir, finalize_lookup
from _load_inventory_data import load_inventory_dir


REQUIRED_HEADERS = ['card_id', 'card_name', 'card_number', 'set_id', 'set_name', 'card_market_value']


def main():
    base = os.path.dirname(__file__)

    lookup_dirs = [
        os.path.join(base, 'card_set_lookup'),
        os.path.join(base, 'card_set_lookup_test'),
    ]

    inventory_dirs = [
        os.path.join(base, 'card_inventory'),
        os.path.join(base, 'card_inventory_test'),
    ]

    # Load lookup data
    for d in lookup_dirs:
        load_lookup_dir(d)
    lookup_df = finalize_lookup()

    # Load inventory data
    inv_frames = []
    for d in inventory_dirs:
        df = load_inventory_dir(d)
        if not df.empty:
            inv_frames.append(df)

    if not inv_frames:
        # Empty inventory: write an empty CSV with required headers and exit
        out_path = os.path.join(base, 'portfolio.csv')
        empty_df = pd.DataFrame(columns=REQUIRED_HEADERS)
        empty_df.to_csv(out_path, index=False)
        print('Inventory empty — wrote empty portfolio with headers to', out_path, file=sys.stderr)
        return

    inventory_df = pd.concat(inv_frames, ignore_index=True)

    # Merge inventory with lookup on card_id (left join to keep all inventory rows)
    if lookup_df is not None and not lookup_df.empty:
        merged = pd.merge(inventory_df, lookup_df[['card_id', 'card_name', 'card_market_value', 'set_name']], on='card_id', how='left')
    else:
        merged = inventory_df.copy()
        merged['card_market_value'] = None
        merged['card_name'] = merged.get('card_name')
        merged['set_name'] = merged.get('set_name')

    # Build final dataframe with readable columns
    final = pd.DataFrame()
    final['card_id'] = merged.get('card_id')
    final['card_name'] = merged.get('card_name')
    final['card_number'] = merged.get('card_number')
    final['set_id'] = merged.get('set_id')
    final['set_name'] = merged.get('set_name')
    final['card_market_value'] = merged.get('card_market_value')

    # Write CSV
    out_path = os.path.join(base, 'portfolio.csv')
    final.to_csv(out_path, index=False)
    print(f'Wrote {len(final)} rows to {out_path}')


if __name__ == '__main__':
    main()
#!/usr/bin/env python3
"""Top-level orchestration script for the Lab 4 portfolio pipeline.

This file imports the small helper modules created in the worksheet and runs
the ETL steps to produce `portfolio.csv`.
"""

import os
from _load_lookup_data import load_lookup_dir, finalize_lookup
from _load_inventory_data import load_inventory_dir
import pandas as pd


def main():
    base = os.path.dirname(__file__)

    # Lookup directories
    lookup_dirs = [
        os.path.join(base, 'card_set_lookup'),
        os.path.join(base, 'card_set_lookup_test'),
    ]

    # Inventory directories
    inventory_dirs = [
        os.path.join(base, 'card_inventory'),
        os.path.join(base, 'card_inventory_test'),
    ]

    # Load lookup data
    for d in lookup_dirs:
        load_lookup_dir(d)

    lookup_df = finalize_lookup()

    # Load inventory data (concatenate both dirs)
    inv_frames = []
    for d in inventory_dirs:
        inv = load_inventory_dir(d)
        if not inv.empty:
            inv_frames.append(inv)

    inventory_df = pd.concat(inv_frames, ignore_index=True) if inv_frames else pd.DataFrame()

    # Merge on card_id
    if not inventory_df.empty and not lookup_df.empty:
        merged = pd.merge(
            inventory_df,
            lookup_df[['card_id', 'card_name', 'card_market_value', 'set_name']],
            on='card_id',
            how='left'
        )

        # Desired output columns (tolerant to missing columns)
        desired = ['card_id', 'card_name', 'card_number', 'set_id', 'set_name', 'card_market_value', 'binder_name', 'page_number', 'slot_number']
        final = merged.reindex(columns=desired).copy()
    elif not lookup_df.empty:
        desired = ['card_id', 'card_name', 'card_number', 'set_id', 'set_name', 'card_market_value']
        final = lookup_df.reindex(columns=desired).copy()
    else:
        final = pd.DataFrame()

    out_path = os.path.join(base, 'portfolio.csv')
    final.to_csv(out_path, index=False)
    print(f'Wrote {len(final)} rows to {out_path}')


if __name__ == '__main__':
    main()
#!/usr/bin/env python3

import os
import json
import pandas as pd



def extract_from_json(obj):
    """Extract fields from a JSON card object (API style)."""
    # defaults
    cid = obj.get('id')
    name = obj.get('name')
    number = obj.get('number')
    set_obj = obj.get('set', {}) or {}
    set_id = set_obj.get('id')
    set_name = set_obj.get('name')

    # tcgplayer prices nested path may or may not exist
    tcg = obj.get('tcgplayer') or {}
    prices = tcg.get('prices') or {}

    normal_market = None
    holofoil_market = None

    # prices keys vary by card; look for 'normal' and 'holofoil'
    if 'normal' in prices and isinstance(prices['normal'], dict):
        normal_market = prices['normal'].get('market')
    if 'holofoil' in prices and isinstance(prices['holofoil'], dict):
        holofoil_market = prices['holofoil'].get('market')

    return {
        'id': cid,
        'name': name,
        'number': number,
        'set_id': set_id,
        'set_name': set_name,
        'normal_market': normal_market,
        'holofoil_market': holofoil_market,
    }


def extract_from_row(row):
    """Extract fields from a CSV/row-like mapping (test files).

    Expected keys: card_name, set_id, card_number but be tolerant.
    """
    cid = None
    name = row.get('card_name') or row.get('name') or row.get('id')
    number = row.get('card_number') or row.get('number')
    set_id = row.get('set_id') or row.get('set')
    set_name = None

    # No tcgplayer data in CSV test files - leave None
    return {
        'id': cid,
        'name': name,
        'number': number,
        'set_id': set_id,
        'set_name': set_name,
        'normal_market': None,
        'holofoil_market': None,
    }


def main():
    base = os.path.dirname(__file__)
    lookup_dirs = [
        os.path.join(base, 'card_set_lookup'),
        os.path.join(base, 'card_set_lookup_test'),
    ]

    records = []

    for d in lookup_dirs:
        if not os.path.isdir(d):
            continue

        for fname in os.listdir(d):
            path = os.path.join(d, fname)
            if not os.path.isfile(path):
                continue

            # try to detect JSON vs CSV
            try:
                with open(path, 'r', encoding='utf-8') as fh:
                    start = fh.read(2048)
            except Exception:
                continue

            is_json = False
            start_strip = start.lstrip()
            if start_strip.startswith('{') or start_strip.startswith('['):
                is_json = True

            if is_json:
                try:
                    with open(path, 'r', encoding='utf-8') as fh:
                        data = json.load(fh)
                except Exception:
                    # skip malformed json
                    continue

                # API returns { 'data': [ ... ] } or a list
                if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
                    for obj in data['data']:
                        records.append(extract_from_json(obj))
                elif isinstance(data, list):
                    for obj in data:
                        records.append(extract_from_json(obj))
                else:
                    # try to interpret single object
                    records.append(extract_from_json(data))
            else:
                # treat as CSV-like (the test file uses CSV but named .json)
                try:
                    df = pd.read_csv(path)
                except Exception:
                    # fallback: parse simple CSV by lines
                    try:
                        with open(path, 'r', encoding='utf-8') as fh:
                            lines = [l.strip() for l in fh if l.strip()]
                        if not lines:
                            continue
                        headers = [h.strip() for h in lines[0].split(',')]
                        for ln in lines[1:]:
                            vals = [v.strip() for v in ln.split(',')]
                            row = dict(zip(headers, vals))
                            records.append(extract_from_row(row))
                        continue
                    except Exception:
                        continue

                for _, row in df.iterrows():
                    records.append(extract_from_row(row.to_dict()))

    if not records:
        print('No card records found in lookup directories.')
        return

    out_df = pd.DataFrame.from_records(records)

    # Create a single readable market value column. Prefer holofoil market if present,
    # otherwise fall back to normal market. If neither exists, value will be None.
    if 'holofoil_market' in out_df.columns and 'normal_market' in out_df.columns:
        out_df['card_market_value'] = out_df['holofoil_market'].combine_first(out_df['normal_market'])
    elif 'holofoil_market' in out_df.columns:
        out_df['card_market_value'] = out_df['holofoil_market']
    elif 'normal_market' in out_df.columns:
        out_df['card_market_value'] = out_df['normal_market']
    else:
        out_df['card_market_value'] = None

    # Rename columns to human-readable names requested by the user.
    rename_map = {
        'id': 'card_id',
        'name': 'card_name',
        'number': 'card_number',
        'set_id': 'set_id',
        'set_name': 'set_name',
        # 'card_market_value' already has the desired name
    }
    out_df = out_df.rename(columns=rename_map)

    # Select and order the columns we want in the lookup dataframe.
    desired_cols = ['card_id', 'card_name', 'card_number', 'set_id', 'set_name', 'card_market_value']
    # Keep only existing desired columns (tolerant if some are missing)
    existing_cols = [c for c in desired_cols if c in out_df.columns]

    # Create a lookup DataFrame (from the API/test lookup files)
    lookup_df = out_df.rename(columns={})[existing_cols].copy() if existing_cols else pd.DataFrame()

    # Ensure card_number and set_id are strings for concatenation
    if not lookup_df.empty:
        if 'card_number' in lookup_df.columns:
            lookup_df['card_number'] = lookup_df['card_number'].astype(str).str.strip()
        if 'set_id' in lookup_df.columns:
            lookup_df['set_id'] = lookup_df['set_id'].astype(str).str.strip()
        # Synthesize card_id in lookup if possible
        if 'set_id' in lookup_df.columns and 'card_number' in lookup_df.columns:
            lookup_df['card_id'] = lookup_df['set_id'] + '-' + lookup_df['card_number']

    # Now load inventory CSVs (manual data you collected) and synthesize card_id there too
    inv_dirs = [os.path.join(base, 'card_inventory'), os.path.join(base, 'card_inventory_test')]
    inv_frames = []
    for d in inv_dirs:
        if not os.path.isdir(d):
            continue
        for fname in os.listdir(d):
            if not fname.lower().endswith('.csv'):
                continue
            path = os.path.join(d, fname)
            try:
                df = pd.read_csv(path, dtype=str)
            except Exception:
                continue
            inv_frames.append(df)

    inventory_df = pd.concat(inv_frames, ignore_index=True) if inv_frames else pd.DataFrame()

    if not inventory_df.empty:
        # normalize column names
        if 'card_number' not in inventory_df.columns and 'number' in inventory_df.columns:
            inventory_df = inventory_df.rename(columns={'number': 'card_number'})
        if 'set_id' not in inventory_df.columns and 'set' in inventory_df.columns:
            inventory_df = inventory_df.rename(columns={'set': 'set_id'})
        # ensure strings and strip
        if 'card_number' in inventory_df.columns:
            inventory_df['card_number'] = inventory_df['card_number'].astype(str).str.strip()
        if 'set_id' in inventory_df.columns:
            inventory_df['set_id'] = inventory_df['set_id'].astype(str).str.strip()
        # synthesize card_id
        if 'set_id' in inventory_df.columns and 'card_number' in inventory_df.columns:
            inventory_df['card_id'] = inventory_df['set_id'] + '-' + inventory_df['card_number']
        else:
            # unable to synthesize card_id
            inventory_df['card_id'] = None


    # Build the final merged dataframe using a helper function. This centralizes the logic
    # for preferring inventory values and bringing in lookup market values.
    def build_final_df(lookup_df, inventory_df):
        """Return a merged final dataframe with columns:
          card_id, card_name, card_number, set_id, set_name, card_market_value
        Prefers inventory values for names/numbers and uses lookup for set_name and market value.
        """

        lk = lookup_df.copy() if lookup_df is not None else pd.DataFrame()
        inv = inventory_df.copy() if inventory_df is not None else pd.DataFrame()

        # normalize and ensure string columns
        for df in (lk, inv):
            for c in ('card_number', 'set_id'):
                if c in df.columns:
                    df[c] = df[c].astype(str).str.strip()

        # synthesize card_id where possible
        if 'set_id' in lk.columns and 'card_number' in lk.columns:
            lk['card_id'] = lk['set_id'] + '-' + lk['card_number']
        if 'set_id' in inv.columns and 'card_number' in inv.columns:
            inv['card_id'] = inv['set_id'] + '-' + inv['card_number']

        # ensure a single market column in lookup (prefer holofoil over normal)
        if not lk.empty:
            if 'card_market_value' not in lk.columns:
                if 'holofoil_market' in lk.columns and 'normal_market' in lk.columns:
                    lk['card_market_value'] = lk['holofoil_market'].combine_first(lk['normal_market'])
                elif 'holofoil_market' in lk.columns:
                    lk['card_market_value'] = lk['holofoil_market']
                elif 'normal_market' in lk.columns:
                    lk['card_market_value'] = lk['normal_market']
                else:
                    lk['card_market_value'] = None

        # Merge: prefer inventory rows (left) and bring lookup fields when available
        if not inv.empty:
            # Use pd.merge to left-join inventory rows with selected lookup columns
            merged = pd.merge(
                inv,
                lk[['card_id', 'card_name', 'set_name', 'card_market_value', 'card_number', 'set_id']],
                on='card_id',
                how='left',
                suffixes=('_inv', '_lk')
            )
        else:
            # present lookup rows if no inventory
            merged = lk.copy()
            for col in ('card_number', 'set_id', 'card_name', 'card_market_value', 'set_name'):
                if col not in merged.columns:
                    merged[col] = None

        # Helper to choose inventory value first, then lookup
        def pick_row_value(row, inv_col, lk_col):
            inv_val = row.get(inv_col)
            if pd.notna(inv_val) and inv_val != '':
                return inv_val
            lk_val = row.get(lk_col)
            return lk_val if pd.notna(lk_val) else None

        final = pd.DataFrame()
        final['card_id'] = merged.get('card_id')

        # card_name: try inventory column names first, then lookup (safe combine)
        if 'card_name_inv' in merged.columns and 'card_name' in merged.columns:
            final['card_name'] = merged['card_name_inv'].combine_first(merged['card_name'])
        elif 'card_name_inv' in merged.columns:
            final['card_name'] = merged['card_name_inv']
        elif 'card_name' in merged.columns:
            final['card_name'] = merged['card_name']
        else:
            final['card_name'] = pd.Series([None] * len(merged))

        # card_number: inventory preferred, otherwise lookup (safe combine)
        if 'card_number' in merged.columns:
            final['card_number'] = merged['card_number']
        elif 'card_number_inv' in merged.columns and 'card_number_lk' in merged.columns:
            final['card_number'] = merged['card_number_inv'].combine_first(merged['card_number_lk'])
        elif 'card_number_inv' in merged.columns:
            final['card_number'] = merged['card_number_inv']
        elif 'card_number_lk' in merged.columns:
            final['card_number'] = merged['card_number_lk']
        else:
            final['card_number'] = pd.Series([None] * len(merged))

        # set_id: inventory preferred, otherwise derive from card_id (safe combine)
        if 'set_id' in merged.columns:
            final['set_id'] = merged['set_id']
        elif 'set_id_inv' in merged.columns and 'set_id_lk' in merged.columns:
            final['set_id'] = merged['set_id_inv'].combine_first(merged['set_id_lk'])
        elif 'set_id_inv' in merged.columns:
            final['set_id'] = merged['set_id_inv']
        elif 'set_id_lk' in merged.columns:
            final['set_id'] = merged['set_id_lk']
        else:
            final['set_id'] = final['card_id'].astype(str).str.split('-', n=1).str[0]

        # set_name and market value come from lookup when available
        final['set_name'] = merged.get('set_name') if 'set_name' in merged.columns else None
        final['card_market_value'] = merged.get('card_market_value') if 'card_market_value' in merged.columns else None

        # keep useful inventory extras if present
        for extra in ('binder_name', 'page_number', 'slot_number'):
            if extra in merged.columns:
                final[extra] = merged[extra]

        return final

    # build the final third dataframe
    final_df = build_final_df(lookup_df, inventory_df)

    # Final cleaning
    # Ensure card_market_value is numeric and fill missing with 0.0
    if 'card_market_value' in final_df.columns:
        final_df['card_market_value'] = pd.to_numeric(final_df['card_market_value'], errors='coerce').fillna(0.0)
    else:
        final_df['card_market_value'] = 0.0

    # Fill missing set_name with 'NOT_FOUND'
    if 'set_name' in final_df.columns:
        final_df['set_name'] = final_df['set_name'].fillna('NOT_FOUND')
    else:
        final_df['set_name'] = 'NOT_FOUND'

    # Create location index by concatenating binder_name, page_number, slot_number
    for loc in ('binder_name', 'page_number', 'slot_number'):
        if loc not in final_df.columns:
            final_df[loc] = ''

    # Make sure values are strings and strip whitespace
    final_df['index'] = (
        final_df['binder_name'].astype(str).str.strip() + '-' +
        final_df['page_number'].astype(str).str.strip() + '-' +
        final_df['slot_number'].astype(str).str.strip()
    )

    # Define final columns to output (only keep those that exist)
    final_cols = [
        'card_id', 'card_name', 'card_number', 'set_id', 'set_name', 'card_market_value',
        'binder_name', 'page_number', 'slot_number', 'index'
    ]
    out_cols = [c for c in final_cols if c in final_df.columns]
    final_df = final_df.loc[:, out_cols].copy()

    # write final portfolio.csv
    out_path = os.path.join(base, 'portfolio.csv')
    final_df.to_csv(out_path, index=False)

    print(f'Wrote {len(final_df)} rows to {out_path} (columns: {list(final_df.columns)})')


if __name__ == '__main__':
    main()

