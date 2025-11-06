// This stream processor captures changes from the 'customerWithAllDataASP' collection
// and emits the processed data to a Kafka topic.
// It includes a filter on the source collection to ensure only documents where 'customerDetails', 'vehicleDetails', and 'customerAgreement' fields exist are processed.
// It transforms the change stream data to include pre- and post-change images along with a timestamp.

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
            "coll": "test",
            "connectionName": "AtlasDatabase",
            "db": "dealer_targetDB"
        }
    }
}

emit = {
  "$emit": {
    "connectionName": "Confluent",
    "topic": "CustomerDetails"
  }
}


deadLetter = {
  dlq: {
    connectionName: "AtlasDatabase",
    db: "ASPDeadLetterDB",
    coll: "dlq"
  }
}
print("Dropping 'customerWithAllDataASP' Stream Processor...")
if (sp.listStreamProcessors({"name":"customerWithAllDataASP"}).length != 0) {sp.customerWithAllDataASP.drop()}
print("Creating 'customerWithAllDataASP' Stream Processor...")
sp.createStreamProcessor("customerWithAllDataASP", [source, af, p, emit], deadLetter)
print("Starting 'customerWithAllDataASP' Stream Processor...")
sp.customerWithAllDataASP.start()