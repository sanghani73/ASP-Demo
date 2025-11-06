from streamProcessorsHelper import create_sp, start_sp, delete_sp
import configparser

# MongoDB Atlas API configuration
config = configparser.ConfigParser()
config.read("config.ini")

WORKSPACE_NAME = config.get("DEFAULT", "WORKSPACE_NAME")

source = {
      "$source": {
        "coll": "customerAgreements",
        "connectionName": "AtlasDatabase",
        "db": "dealer_sourceDB"
      }
    }

m = {
    "$match": {
      "operationType": "insert"
    }
  }

p = {
        "$project": {
          "_id": "$fullDocument.customerID", 
          "customerAgreement": {"$concatArrays": [["$fullDocument"], []]},
          "latestAgreement": "$fullDocument"
        }
    }

merge = {
      "$merge": {
        "into": {
            "coll": "customerWithAllDataASP",
            "connectionName": "AtlasDatabase",
            "db": "dealer_targetDB"
        },
        "whenMatched": [
          {
            "$set": {
              "customerAgreement" : { "$cond":
                {
                  "if": {"$isArray": "$customerAgreement" },
                  "then": { "$concatArrays": [ "$customerAgreement", "$$new.customerAgreement" ] },
                  "else": {"$concatArrays": ["$$new.customerAgreement",[]]}
                }
              },
              "latestAgreement": "$$new.latestAgreement"
            }
          }
        ]
    }
}

deadLetter = {
  "dlq": {
    "connectionName": "AtlasDatabase",
    "db": "ASPDeadLetterDB",
    "coll": "dlq"
  }, "tier": "SP2"
}

SP_CONFIG = {
    "name": "customerAgreements",
    "pipeline": [source, m, p, merge],
    "options": deadLetter
}


def main():
    print(f"Deleting existing stream processor {SP_CONFIG['name']} if it exists...")
    delete_sp(WORKSPACE_NAME, SP_CONFIG['name'])
    print(f"Creating stream processing for {SP_CONFIG['name']}...")
    create_sp(WORKSPACE_NAME, SP_CONFIG)
    print(f"Starting stream processor {SP_CONFIG['name']}...")
    start_sp(WORKSPACE_NAME, SP_CONFIG['name'])

if __name__ == "__main__":
    main()  