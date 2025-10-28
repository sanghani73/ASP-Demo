# Ensure dependencies are installed: `pip install pymongo`
# This script modifies a MongoDB collection to enable pre- and post-image capturing for change streams.

import configparser
from pymongo import MongoClient  
config = configparser.ConfigParser()
config.read("config.ini")
MONGODB_URI = config["DEFAULT"]["MONGODB_URI"]
TARGET_DB_NAME = "dealer_targetDB"

client = MongoClient(MONGODB_URI)  

def modifyCollectionPrePostImage(db_name, coll):
    print(f"Modifying collection {coll} in DB {db_name} to enable pre- and post-images...")
    db = client[db_name]
    if coll not in db.list_collection_names():
        db.create_collection(coll)

    db.command( {
        "collMod": coll,
        "changeStreamPreAndPostImages": { "enabled": True }
    } )

if __name__ == "__main__":
    modifyCollectionPrePostImage(TARGET_DB_NAME, "customerWithAllDataASP")
    print("Done.")