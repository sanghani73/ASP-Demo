// This stream processor captures changes from the 'customerVehicles' collection
// and merges vehicle details into the 'customerWithAllDataASP' collection.
// It uses a cached lookup to fetch vehicle details based on vehicleID.
// The vehicle details are added to an array field in the target collection for each customer instead of overwriting existing data.

source = {
      "$source": {
        "coll": "customerVehicles",
        "connectionName": "AtlasDatabase",
        "db": "dealer_sourceDB"
      }
    }
m = {
    "$match": {
      "operationType": "insert"
    }
  }
lk =  {
        "$cachedLookup": {
            "ttl": {
              "size": 30,
              "unit": "minute"
            },
            "from": {"connectionName": "AtlasDatabase", "db": "dealer_sourceDB", "coll": "vehicles"}, 
            "localField": "fullDocument.vehicleID", 
            "foreignField": "_id", 
            "as": "vehicles"
        }
    } 
 uw = {
        "$unwind": {
            "path": "$vehicles"
        }
    }
p = {
        "$project": {
          "_id": "$fullDocument.customerID", 
          "vehicleDetails": {"$concatArrays": [["$vehicles"], []]},
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
            $set: {
              vehicleDetails : { $cond:
                {
                  if: {$isArray: "$vehicleDetails" },
                  then: { $concatArrays: [ "$vehicleDetails", "$$new.vehicleDetails" ] },
                  else: {$concatArrays: ["$$new.vehicleDetails",[]]}
                }
              }
            }
          }
        ]
    }
}

deadLetter = {
  dlq: {
    connectionName: "AtlasDatabase",
    db: "ASPDeadLetterDB",
    coll: "dlq"
  }
}
print("Dropping 'customerVehicles' Stream Processor...")
if (sp.listStreamProcessors({"name":"customerVehicles"}).length != 0) {sp.customerVehicles.drop()}
print("Creating 'customerVehicles' Stream Processor...")
sp.createStreamProcessor("customerVehicles", [source, m, lk, uw, p, merge], deadLetter)
print("Starting 'customerVehicles' Stream Processor...")
sp.customerVehicles.start()