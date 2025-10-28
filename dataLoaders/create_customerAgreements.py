# ensure dependency is installed - pip install pymongo  
# Create records in MongoDB that represents associations between customers and agreements

import random, configparser
from datetime import datetime  
from pymongo import MongoClient  
  
config = configparser.ConfigParser()
config.read("config.ini")
MONGODB_URI = config["DEFAULT"]["MONGODB_URI"]
DB_NAME = "dealer_sourceDB"

client = MongoClient(MONGODB_URI)  
db = client[DB_NAME]  
customers = db["customers"]  
agreement = db["customerAgreements"]  
  
id_counters = {  
    "customerAgreements": 1,  
}  
def next_id(coll):   
    i = id_counters[coll]; id_counters[coll] += 1; return i  

def create_agreement(customerID):  
    doc = {  
        "_id": next_id("customerAgreements"),  
        "customerID": customerID,  
        "version": random.choice(["v1.0","v1.1","v2.0"]),  
        "agreed": random.choice([True, False]),  
        "agreedAt": datetime.utcnow(),  
    }  
    agreement.insert_one(doc)  

    print(f"Inserted customerAgreement {doc['_id']} for customer {doc['customerID']}")  

# delete collections before running if they exist
def drop_collections_if_exist():
    if "customerAgreements" in db.list_collection_names():
        agreement.drop()
    print("Dropped existing collections.")

if __name__ == "__main__":  
    print(f"Connecting to {MONGODB_URI}, DB={DB_NAME} and creating agreement agreements...")  
    drop_collections_if_exist()
    for cust in customers.find({}, {"_id": 1}):
        create_agreement(cust["_id"])
        print("loaded agreement for customer ", cust["_id"])
    # create index on customerID for faster lookups if not exists
    agreement.create_index("customerID")