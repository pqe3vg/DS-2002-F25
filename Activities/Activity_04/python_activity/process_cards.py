#!/usr/bin/env python3

import sys
import json
import csv

try:
    data = json.loads(sys.stdin.read())
except json.JSONDecodeError:
    print("Error: Invalid JSON received from the pipe.", file=sys.stderr)
    sys.exit(1)

fieldnames = ['card_id', 'card_name', 'set_name', 'rarity', 'market_price']
writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, restval="N/A")
writer.writeheader()

cards = data['data']

for card in cards:
    writer.writerow({
        'card_id': card.get('id', 'N/A'),
        'card_name': card.get('name', 'N/A'),
        'set_name': card.get('set', {}).get('name', 'N/A'),
        'rarity': card.get('rarity', 'N/A'),
        'market_price': card.get('tcgplayer', {}).get('prices', {}).get('holofoil', {}).get('market', 'N/A')
    })


