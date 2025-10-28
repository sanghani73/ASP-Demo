// This stream processor captures changes from the 'customers' collection
// and merges customer details into the 'customerWithAllDataASP' collection.
// It transforms the data by concatenating the customer first and last names and elevates certain fields to the root of the document.

source = {
      "$source": {
        "coll": "customers",
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
            "_id": "$fullDocument._id",
            "name": { "$concat": ["$fullDocument.firstName", " ", "$fullDocument.lastName"] },
            "customerDetails.firstName": "$fullDocument.firstName",
            "customerDetails.lastName": "$fullDocument.lastName",
            "customerDetails.email": "$fullDocument.email",
            "customerDetails.phone": "$fullDocument.phone",
            "CustomerCreatedAt": "$fullDocument.createdAt",
            "isDeleted": "$fullDocument.isDeleted",
            "CustomerLastModified": "$fullDocument.lastModified"
        }
    }
merge = {
      "$merge": {
        "into": {
            "coll": "customerWithAllDataASP",
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
print("Dropping 'customers' Stream Processor...")
if (sp.listStreamProcessors({"name":"customers"}).length != 0) {sp.customers.drop()}
print("Creating 'customers' Stream Processor...")
sp.createStreamProcessor("customers", [source, m, p, merge], deadLetter)
print("Starting 'customers' Stream Processor...")
sp.customers.start()