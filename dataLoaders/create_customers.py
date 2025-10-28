# ensure dependency is installed - pip install pymongo  
# Create customer records in MongoDB

import random, configparser
from datetime import datetime  
from pymongo import MongoClient  

config = configparser.ConfigParser()
config.read("config.ini")
MONGODB_URI = config["DEFAULT"]["MONGODB_URI"]
DB_NAME = "dealer_sourceDB"
CUSTOMER_COUNT = 10

client = MongoClient(MONGODB_URI)  
db = client[DB_NAME]  
customers = db["customers"]  
  
id_counters = {  
    "customers": 1,  
}  
def next_id(coll):   
    i = id_counters[coll]; id_counters[coll] += 1; return i  
  
first_names = ["Alex","Jordan","Sam","Taylor","Casey","Riley","Morgan","Avery"]  
last_names = ["Smith","Johnson","Lee","Brown","Davis","Martinez","Clark","Nguyen"]  
  
customer_ids = []  
  
def create_customer():  
    _id = next_id("customers")  
    fn, ln = random.choice(first_names), random.choice(last_names)  
    doc = {  
        "_id": _id,  
        "firstName": fn,  
        "lastName": ln,  
        "email": f"{fn}.{ln}{random.randint(100,999)}@example.com".lower(),  
        "phone": f"+1-555-{random.randint(100,999)}-{random.randint(1000,9999)}",  
        "createdAt": datetime.utcnow(), 
        "isDeleted": False,
        "lastModified": datetime.utcnow()
    }  
    customers.insert_one(doc); customer_ids.append(_id)  
    print(f"Inserted Customer {_id}")  
  
# delete collections before running if they exist
def drop_collections_if_exist():
    if "customers" in db.list_collection_names():
        customers.drop()
    print("Dropped existing collections.")

def modifyCollectionPrePostImage():
    db.runCommand( {
        "collMod": "customers",
        "changeStreamPreAndPostImages": { "enabled": True }
    } )

if __name__ == "__main__":
    print(f"Connecting to {MONGODB_URI}, DB={DB_NAME} and creating {CUSTOMER_COUNT} customers...")
    drop_collections_if_exist()
    for _ in range(CUSTOMER_COUNT): 
        create_customer()  
        # time.sleep(random.uniform(1, 5))  # simulate delay
        
    modifyCollectionPrePostImage
    print("Done.")