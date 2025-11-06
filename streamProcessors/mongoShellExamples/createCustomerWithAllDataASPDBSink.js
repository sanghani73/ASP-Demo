// This stream processor captures changes from the 'customerWithAllDataASP' collection
// and writes the processed data into the 'DBSinkCustomerDetails' collection in the same database.
// It includes a filter on the source collection to ensure only documents where 'customerDetails', 'vehicleDetails', and 'customerAgreement' fields exist are processed.
// It uses the same pipeline as the stream processor that emits to Kafka, but instead merges the data into a MongoDB collection.

source = {
      "$source": {
        "coll": "customerWithAllDataASP",
        "connectionName": "AtlasDatabase",
        "db": "dealer_targetDB",
        "config": { "fullDocumentBeforeChange" : "whenAvailable", 
                    "fullDocument" : "required",
                    "pipeline": [{'$match': {
                                      'fullDocument.customerDetails': {'$exists': true}, 
                                      'fullDocument.vehicleDetails': {'$exists': true}, 
                                      'fullDocument.customerAgreement': {'$exists': true}
                                      }
                                }] 
                  }
          }
}

af = {
  "$addFields": {
    "time": {
      "$currentDate": {}
    }
  }
}
p = {
        "$project": {
            "_id": "$fullDocument._id",
            "time": 1,
            "data": {
              "BeforePayload": "$fullDocumentBeforeChange",
              "AfterPayload": "$fullDocument",
              "operationType": "$operationType"
            }
        }
    }

merge = {
      "$merge": {
        "into": {
            "coll": "DBSinkCustomerDetails",
            "connectionName": "AtlasDatabase",
            "db": "dealer_targetDB"
        }
    }
}

deadLetter = {
  dlq: {
    connectionName: "AtlasDatabase",
    db: "ASPDeadLetterDB",
    coll: "dlq"
  }
}
print("Dropping 'customerWithAllDataASPDBSink' Stream Processor...")
if (sp.listStreamProcessors({"name":"customerWithAllDataASPDBSink"}).length != 0) {sp.customerWithAllDataASPDBSink.drop()}
print("Creating 'customerWithAllDataASPDBSink' Stream Processor...")
sp.createStreamProcessor("customerWithAllDataASPDBSink", [source, af, p, merge], deadLetter)
print("Starting 'customerWithAllDataASPDBSink' Stream Processor...")
sp.customerWithAllDataASPDBSink.start()