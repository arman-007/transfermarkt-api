from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import requests
import json
import argparse
from dotenv import load_dotenv
import os
load_dotenv()  # Load environment variables from a .env file if present

# MongoDB connection details (update these)
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

def convert_objectid_to_str(data):
    """
    Recursively converts all ObjectId and datetime fields in the dictionary to strings.
    """
    if isinstance(data, dict):
        return {key: convert_objectid_to_str(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_objectid_to_str(item) for item in data]
    elif isinstance(data, ObjectId):
        return str(data)
    elif isinstance(data, datetime):
        return data.isoformat()  # Convert datetime to ISO string
    else:
        return data

def fetch_players_from_db(player_names: list):
    """Fetch player data from MongoDB based on display_name or name."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Query MongoDB for all players by display_name or name in one go
        query = {
            "$or": [
                {"display_name": {"$in": player_names}},
                {"name": {"$in": player_names}}
            ]
        }

        players = collection.find(query)
        
        # Build a dictionary where both display_name and name can be used as keys
        player_data_dict = {}

        for player in players:
            # Use display_name as the primary key, and name as a fallback
            player_data_dict[player["display_name"].strip()] = player
            if player["name"].strip() != player["display_name"].strip():
                player_data_dict[player["name"].strip()] = player

        return player_data_dict
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None


def fetch_injury_data(url: str):
    """Fetch injury data from FastAPI endpoint."""
    api_url = "http://localhost:8000/players/injuries"
    payload = {"url": url}

    # Make a POST request to the FastAPI endpoint
    response = requests.post(api_url, json=payload)
    
    if response.status_code == 200:
        print(f"Successfully fetched data for: {url}")
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

def main():
    # Command-line argument parsing
    parser = argparse.ArgumentParser(description="Fetch injury data for a league.")
    parser.add_argument("url", type=str, help="URL of the league injuries page")
    args = parser.parse_args()

    # Fetch injury data
    data = fetch_injury_data(args.url)
    
    if data:
        player_names = [player_info["player"]["name"] for player_info in data.get("rows", [])]

        # Fetch player data from MongoDB (both display_name and name)
        player_data_dict = fetch_players_from_db(player_names)

        all_player_data = []  # List to collect all players' data

        # Loop through each player in the injuries list
        for player_info in data.get("rows", []):
            player_name = player_info["player"]["name"]

            # Look up the player data from MongoDB using the name (display_name match)
            player_data = player_data_dict.get(player_name)

            if not player_data:
                print(f"Player {player_name} not found in the database.")
                continue  # Skip to the next player if not found

            # Convert any ObjectId or datetime fields to strings
            player_data = convert_objectid_to_str(player_data)
            player_info = convert_objectid_to_str(player_info)

            # Merge player data and injury data
            merged_data = {**player_data, **player_info}

            # Append the merged data to the list
            all_player_data.append(merged_data)
            print(f"Added injury data for {player_data['name']}.")

        # Save all merged data to a single JSON file
        with open("output/all_injured_players_data_2.json", "w") as outfile:
            json.dump(all_player_data, outfile, indent=2)
        print(f"Saved all merged data to all_injured_players_data.json.")

if __name__ == "__main__":
    main()
