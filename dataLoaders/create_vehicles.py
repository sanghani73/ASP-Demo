# ensure dependency is installed - pip install pymongo  
# Create vehicle records in MongoDB as reference data for customer-vehicle associations

import random, configparser
from datetime import datetime  
from pymongo import MongoClient  

config = configparser.ConfigParser()
config.read("config.ini")
MONGODB_URI = config["DEFAULT"]["MONGODB_URI"]
DB_NAME = "dealer_sourceDB"
VEHICLE_COUNT = 1000

client = MongoClient(MONGODB_URI)  
db = client[DB_NAME]  
vehicles = db["vehicles"]  
  
id_counters = {  
    "vehicles": 1,  
}  
def next_id(coll):   
    i = id_counters[coll]; id_counters[coll] += 1; return i  
  
makes = ["Toyota","Ford","Honda","Chevrolet","Nissan","BMW","Audi","Kia"]  
models = ["Camry","F-150","Civic","Silverado","Altima","3 Series","A4","Sorento"]  
chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"  
def rand_vin(): return "".join(random.choices(chars, k=17))  
  
vehicle_ids = []
  
def create_vehicle():  
    _id = next_id("vehicles")  
    doc = {  
        "_id": _id,  
        "vin": rand_vin(),  
        "make": random.choice(makes),  
        "model": random.choice(models),  
        "year": random.randint(2005, 2025),  
        "createdAt": datetime.utcnow(),  
    }  
    vehicles.insert_one(doc); vehicle_ids.append(_id)  

# delete collections before running if they exist
def drop_collections_if_exist():
    if "vehicles" in db.list_collection_names():
        vehicles.drop()
    print("Dropped existing collections.")

if __name__ == "__main__":
    print(f"Connecting to {MONGODB_URI}, DB={DB_NAME} and creating {VEHICLE_COUNT} vehicles...")
    drop_collections_if_exist()
    for _ in range(VEHICLE_COUNT): create_vehicle()  
    print("Done.")