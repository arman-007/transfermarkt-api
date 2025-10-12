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

def fetch_player_from_db(player_name: str):
    """Fetch player data from MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Query MongoDB first by display_name
        player_data = collection.find_one({"display_name": player_name})
        
        # If not found, try searching by name
        if not player_data:
            print(f"Player {player_name} not found by display_name, trying name...")
            player_data = collection.find_one({"name": player_name})

        if player_data:
            print(f"Found player: {player_name}")
            # Convert ObjectId to string
            return convert_objectid_to_str(player_data)
        else:
            print(f"Player {player_name} not found in the database.")
            return None
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
        # Loop through each player in the injuries list
        for player_info in data.get("rows", []):
            player_name = player_info["player"]["name"]

            # Fetch the player data from MongoDB
            player_data = fetch_player_from_db(player_name)

            if player_data:
                # Convert any ObjectId or datetime fields to strings
                player_data = convert_objectid_to_str(player_data)
                player_info = convert_objectid_to_str(player_info)

                # Merge player data and injury data
                merged_data = {**player_data, **player_info}
                
                # Save the merged data to a JSON file (or print it for now)
                with open(f"output/{player_name}_injury_data.json", "w") as outfile:
                    json.dump(merged_data, outfile, indent=2)
                print(f"Saved injury data for {player_data['name']}.")


if __name__ == "__main__":
    main()
