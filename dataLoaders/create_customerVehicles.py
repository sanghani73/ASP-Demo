# ensure dependency is installed - pip install pymongo  
# Create records in MongoDB that represents associations between customers and vehicles

import random, configparser
from datetime import datetime  
from pymongo import MongoClient  

config = configparser.ConfigParser()
config.read("config.ini")
MONGODB_URI = config["DEFAULT"]["MONGODB_URI"]
DB_NAME = "dealer_sourceDB"
  
client = MongoClient(MONGODB_URI)  
db = client[DB_NAME]  
vehicles = db["vehicles"]  
customers = db["customers"]
customerVehicles = db["customerVehicles"]  
  
id_counters = {  
    "customerVehicles": 1
}  
def next_id(coll):   
    i = id_counters[coll]; id_counters[coll] += 1; return i  
  
def create_association(customer_id, vehicle_id):  
    doc = {  
        "_id": next_id("customerVehicles"),  
        "customerID": customer_id,
        "vehicleID": vehicle_id,
        "associationType": random.choice(["owner","co-owner","driver"]),  
        "status": random.choice(["active","inactive"]),  
        "startDate": datetime.utcnow(),  
    }  
    customerVehicles.insert_one(doc)      
    print(f"Inserted CustomerVehicleAssociation {doc['_id']} for customer {doc['customerID']} and vehicle {doc['vehicleID']}")  
  
# delete collections before running if they exist
def drop_collections_if_exist():
    if "customerVehicles" in db.list_collection_names():
        customerVehicles.drop()
    print("Dropped existing collections.")

if __name__ == "__main__":  
    print(f"Connecting to {MONGODB_URI}, DB={DB_NAME} and creating customer-vehicle associations and privacy agreements...")  
    drop_collections_if_exist()
    
    cars = list(db["vehicles"].aggregate([{"$sample": {"size": 10}},{"$project": {"_id": 1}}]))

    for cust in customers.find({}, {"_id": 1}):
        veh = cars.pop()
        create_association(cust["_id"], veh["_id"])
