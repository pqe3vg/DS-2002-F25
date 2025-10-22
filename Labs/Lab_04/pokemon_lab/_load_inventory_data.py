#!/usr/bin/env python3


import os
import pandas as pd


def load_inventory_dir(inventory_dir):

	inventory_data = []

	if not os.path.isdir(inventory_dir):
		return pd.DataFrame()

	for fname in os.listdir(inventory_dir):
		if not fname.lower().endswith('.csv'):
			continue
		path = os.path.join(inventory_dir, fname)
		if not os.path.isfile(path):
			continue
		try:
			# read as strings to preserve formatting (card numbers, leading zeros)
			df = pd.read_csv(path, dtype=str)
		except Exception:
			# skip files that fail to parse
			continue
		inventory_data.append(df)

	if not inventory_data:
		return pd.DataFrame()

	inventory_df = pd.concat(inventory_data, ignore_index=True)

	# Normalize common column names
	if 'number' in inventory_df.columns and 'card_number' not in inventory_df.columns:
		inventory_df = inventory_df.rename(columns={'number': 'card_number'})
	if 'set' in inventory_df.columns and 'set_id' not in inventory_df.columns:
		inventory_df = inventory_df.rename(columns={'set': 'set_id'})

	# Ensure card_number and set_id are strings and trimmed
	if 'card_number' in inventory_df.columns:
		inventory_df['card_number'] = inventory_df['card_number'].astype(str).str.strip()
	if 'set_id' in inventory_df.columns:
		inventory_df['set_id'] = inventory_df['set_id'].astype(str).str.strip()

	# Create unified key: set_id-card_number
	if 'set_id' in inventory_df.columns and 'card_number' in inventory_df.columns:
		inventory_df['card_id'] = inventory_df['set_id'] + '-' + inventory_df['card_number']
	else:
		inventory_df['card_id'] = None

	return inventory_df
