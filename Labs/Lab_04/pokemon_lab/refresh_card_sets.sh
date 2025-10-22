#!/bin/bash
echo "Refreshing all card sets in card_set_lookup/"
for FILE in card_set_lookup/*.json; do
    if [ ! -e "$FILE" ]; then
        echo "No JSON files found in card_set_lookup/."
        exit 0
    fi

    SET_ID=$(basename "$FILE" .json)

    echo "Updating card set: $SET_ID..."

    curl -s "https://api.pokemontcg.io/v2/cards?q=set.id:${SET_ID}" -o "$FILE"

    echo "Data for $SET_ID has been written to $FILE."
done

echo "All card sets have been refreshed successfully!"
