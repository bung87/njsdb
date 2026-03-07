# NJSDB

![](https://img.shields.io/badge/status-beta-orange)
![](https://img.shields.io/badge/platforms-native%20only-orange)

A very simple NoSQL JSON document database written on top of SQLite.

## Usage

```nim
import njsdb
import json

# Create a database instance and open a connection
var db = NJSDB()
db.open("database.db")

# Select a collection (table) to work with
db.collection("documents")

# Write a document
db.put(%* {
    "id": "1234",
    "timestamp": 123456,
    "type": "example",
    "text": "Hello world!"
})

# Get a specific document by it's ID (null if not found)
var doc = db.get("1234")

# Fetch a document with a query
var doc = db.query().where("type", "==", "example").get()

# Fetch a list of documents with a query
var docs = db.query()
    .where("timestamp", ">=", 1000)
    .where("timestamp", "<=", 2000)
    .limit(5)
    .offset(2)
    .list()

# Upsert: Insert or update document
db.upsert(%* {
    "id": "1234",
    "timestamp": 123456,
    "type": "example",
    "text": "Updated text!"
})
# Returns: true if inserted, false if updated

# Upsert with merge: Update by merging fields if exists, insert if not
db.upsert(%* {
    "id": "1234",
    "text": "Only update this field"
}, merge = true)

# Delete documents
db.remove("1234")
db.query().where("type", "==", "example").remove()

# Batch modifications
db.batch do():
    db.put(%* { "name": "item1" })
    db.put(%* { "name": "item2" })
    db.put(%* { "name": "item3" })

# Close the database
db.close()
```

## Collections

NJSDB supports multiple collections (tables) in the same database. Each collection is stored in a separate SQLite table, providing data isolation and avoiding table-level lock contention.

### Selecting a Collection

```nim
# You must select a collection before any operations
db.collection("users")
db.put(%* { "id": "u1", "name": "Alice" })

# Switch to another collection
db.collection("orders")
db.put(%* { "id": "o1", "total": 100 })

# Method chaining is supported
db.collection("products")
   .query()
   .where("price", ">", 50)
   .list()
```

### Collection Isolation

```nim
# Data is isolated between collections
db.collection("active_users")
db.put(%* { "id": "user1", "status": "active" })

db.collection("archived_users")
db.put(%* { "id": "user1", "status": "archived" })

# Query only returns documents from the current collection
let active = db.collection("active_users").get("user1")
echo active["status"].getStr  # "active"

let archived = db.collection("archived_users").get("user1")
echo archived["status"].getStr  # "archived"
```

### Important Note

You **must** call `collection()` before any database operation. If you don't, a `NJSDBError` will be raised:

```nim
db.open("database.db")
# Missing: db.collection("...")
db.put(%* { "id": "test" })  # Raises NJSDBError: No collection selected
```

## Query Operators

### Comparison Operators

```nim
# Basic comparisons
db.query().where("age", ">", 18).list()
db.query().where("status", "==", "active").list()
db.query().where("score", ">=", 100).list()
```

### MongoDB-style Filters

```nim
# Using filter() with JSON objects
let filter = %*{
    "status": "active",
    "age": { "$gte": 18 }
}
db.query().filter(filter).list()

# $in operator
let filter = %*{
    "type": { "$in": ["A", "B", "C"] }
}
db.query().filter(filter).list()

# $exists operator
let filter = %*{
    "deletedAt": { "$exists": false }
}
db.query().filter(filter).list()

# $type operator
let filter = %*{
    "score": { "$type": "number" }
}
db.query().filter(filter).list()
# Supported types: "string", "number", "boolean", "array", "object", "null"

# $regex operator (basic pattern matching)
let filter = %*{
    "name": { "$regex": "^Jo.*" }
}
db.query().filter(filter).list()
```

### Logical Operators

```nim
# $or operator
let filter = %*{
    "$or": [
        { "status": "active" },
        { "priority": { "$gt": 5 } }
    ]
}
db.query().filter(filter).list()

# $and operator
let filter = %*{
    "$and": [
        { "type": "A" },
        { "status": "active" }
    ]
}
db.query().filter(filter).list()

# Mixed operators
let filter = %*{
    "status": "active",
    "$or": [
        { "type": "A" },
        { "priority": { "$gt": 5 } }
    ]
}
db.query().filter(filter).list()
```

### Array Operators

```nim
# $all - Array contains all specified values
let filter = %*{
    "tags": { "$all": ["important", "urgent"] }
}
db.query().filter(filter).list()

# $size - Array has specific length
let filter = %*{
    "tags": { "$size": 3 }
}
db.query().filter(filter).list()
```

### Nested Field Queries

```nim
# Query nested objects using dot notation
db.query().where("address.city", "==", "New York").list()
db.query().where("profile.age", ">", 25).list()

# Using filter with nested fields
let filter = %*{
    "address.zipcode": "10001",
    "profile.settings.theme": "dark"
}
db.query().filter(filter).list()
```

## Update Operators

```nim
# $set - Set field values
db.query().where("id", "==", "doc1").update(%*{
    "$set": { "name": "Updated", "status": "active" }
})

# $inc - Increment numeric values
db.query().where("id", "==", "doc1").update(%*{
    "$inc": { "views": 1, "likes": 1 }
})

# $mul - Multiply numeric values
db.query().where("id", "==", "doc1").update(%*{
    "$mul": { "price": 1.1 }  # 10% increase
})

# $unset - Remove fields
db.query().where("id", "==", "doc1").update(%*{
    "$unset": { "tempField": "" }
})

# $rename - Rename fields
db.query().where("id", "==", "doc1").update(%*{
    "$rename": { "oldName": "newName" }
})

# Combined operators
db.query().where("id", "==", "doc1").update(%*{
    "$inc": { "counter": 1 },
    "$set": { "updatedAt": 123456 }
})
```

## Sorting, Limiting, and Pagination

```nim
# Sort results
db.query().sort("name", ascending = true).list()
db.query().sort("age", ascending = false, isNumber = true).list()

# Limit and offset
db.query().limit(10).list()  # First 10 documents
db.query().offset(10).limit(10).list()  # Documents 11-20

# Combined
db.query()
    .where("status", "==", "active")
    .sort("createdAt", ascending = false)
    .limit(20)
    .list()
```

## Projection (Field Selection)

```nim
# Include only specific fields
let projection = %*{ "name": 1, "email": 1 }
db.query().project(projection).list()

# Exclude specific fields
let projection = %*{ "password": 0, "secretKey": 0 }
db.query().project(projection).list()
```

## Aggregation and Counting

```nim
# Count documents
db.query().count()
db.query().where("status", "==", "active").count()

# Get distinct values
db.query().distinctValues("category")

# Aggregate count by field
db.aggregateCount("documents", "category")

# Extended aggregation with multiple operators
let result = db.aggregate("category", %*{
    "$sum": "amount",
    "$avg": "price",
    "$min": "stock",
    "$max": "stock"
})

# Access aggregation results
for agg in result:
    echo "Category: ", agg.groupId
    echo "Count: ", agg.count
    echo "Total: ", agg.sum
    echo "Average: ", agg.avg
    echo "Min: ", agg.min
    echo "Max: ", agg.max

# Aggregate with filter
let filter = %*{ "status": "active" }
let result = db.aggregate("category", %*{"$sum": "amount"}, filter)
```

## MongoDB-style Aggregation Pipeline

NJSDB now supports MongoDB-style aggregation pipelines with `$match`, `$group`, `$sort`, `$limit`, and `$skip` stages.

```nim
# Basic aggregation pipeline: match and group
let result = db.collection("orders").aggregate(@[
  %*{ "$match": { "status": "completed" } },
  %*{ "$group": { "_id": "$customerId", "total": { "$sum": "$amount" } } }
])

for doc in result.data:
  echo "Customer: ", doc["_id"].getStr
  echo "Total: ", doc["total"].getFloat

# Count documents per group
let result = db.collection("orders").aggregate(@[
  %*{ "$group": { "_id": "$customerId", "orderCount": { "$sum": 1 } } }
])

# Multiple aggregation operators
let result = db.collection("sales").aggregate(@[
  %*{ "$match": { "year": 2024 } },
  %*{ "$group": { 
    "_id": "$region", 
    "totalSales": { "$sum": "$amount" },
    "avgSale": { "$avg": "$amount" },
    "minSale": { "$min": "$amount" },
    "maxSale": { "$max": "$amount" }
  } },
  %*{ "$sort": { "totalSales": -1 } },
  %*{ "$limit": 10 }
])

# Pipeline without grouping (just filter, sort, limit)
let result = db.collection("products").aggregate(@[
  %*{ "$match": { "category": "electronics" } },
  %*{ "$sort": { "price": -1 } },
  %*{ "$limit": 5 }
])

# Complex match conditions
let result = db.collection("orders").aggregate(@[
  %*{ "$match": { 
    "status": { "$ne": "cancelled" },
    "amount": { "$gte": 100 }
  } },
  %*{ "$group": { "_id": "$customerId", "total": { "$sum": "$amount" } } }
])
```

## Query Explain

```nim
# Analyze query performance
let plan = db.query()
    .where("field", "==", "value")
    .explain()
# Returns query plan with operation details
```

## Iterator

```nim
# Memory-efficient iteration
for doc in db.query().where("status", "==", "active").list():
    echo doc["name"].getStr
```

## Bulk Operations

```nim
# Bulk insert - much faster than individual put() calls
let docs = @[
    %*{ "name": "Item 1", "value": 10 },
    %*{ "name": "Item 2", "value": 20 },
    %*{ "name": "Item 3", "value": 30 }
]
let inserted = db.bulkInsert(docs)

# Bulk delete by IDs
let idsToDelete = @["id1", "id2", "id3"]
let deleted = db.bulkDelete(idsToDelete)
```

See [tests.nim](tests/tests.nim) for more examples.
