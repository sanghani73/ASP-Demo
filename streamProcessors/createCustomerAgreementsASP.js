// This stream processor captures changes from the 'customerAgreements' collection
// and merges agreement details into the 'customerWithAllDataASP' collection.
// It appends the new agreement to an array of agreements for each customer and updates the latest agreement details.

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
            "db": "dealer_targetDB",
        },
        "whenMatched": [
          {
            $set: {
              customerAgreement : { $cond:
                {
                  if: {$isArray: "$customerAgreement" },
                  then: { $concatArrays: [ "$customerAgreement", "$$new.customerAgreement" ] },
                  else: {$concatArrays: ["$$new.customerAgreement",[]]}
                }
              },
              latestAgreement: "$$new.latestAgreement"
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
print("Dropping 'customerAgreements' Stream Processor...")
if (sp.listStreamProcessors({"name":"customerAgreements"}).length != 0) {sp.customerAgreements.drop()}
print("Creating 'customerAgreements' Stream Processor...")
sp.createStreamProcessor("customerAgreements", [source, m, p, merge], deadLetter)
print("Starting 'customerAgreements' Stream Processor...")
sp.customerAgreements.start()