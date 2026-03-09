# NJSDB

[![Tests](https://github.com/bung87/njsdb/actions/workflows/test.yml/badge.svg)](https://github.com/bung87/njsdb/actions/workflows/test.yml)
![](https://img.shields.io/badge/status-beta-orange)
![](https://img.shields.io/badge/platforms-native%20only-orange)

A simple NoSQL JSON document database written in Nim, built on top of SQLite.

## Features

- **JSON Document Storage**: Store and retrieve JSON documents with automatic indexing
- **MongoDB-style Query API**: Familiar query syntax with filters, operators, and aggregation
- **Collections**: Multi-table support for data isolation
- **Nested Field Queries**: Query nested objects using dot notation
- **Update Operators**: `$set`, `$inc`, `$mul`, `$unset`, `$rename`
- **Aggregation Pipeline**: MongoDB-style `$match`, `$group`, `$sort`, `$limit`, `$skip`, `$project`, `$count`
- **Bulk Insert**: Efficient batch insert with `insertMany()`
- **Transactions**: Transaction wrapper with `withTransaction()`
- **Query Explain**: Analyze query performance with `explain()`
- **Projection**: Field selection (include/exclude) with `project()`

## Installation

Add to your `.nimble` file:

```nim
requires "njsdb >= 0.1.2"
```

Or install via nimble:

```bash
nimble install njsdb
```

## Quick Start

```nim
import njsdb
import json

# Create a database instance and open a connection
var db = NJSDB()
db.open("database.db")  # Use ":memory:" for in-memory database

# Select a collection to work with
db.collection("documents")

# Insert a document
db.put(%*{
    "id": "1234",
    "timestamp": 123456,
    "type": "example",
    "text": "Hello world!"
})

# Get a specific document by ID (returns nil if not found)
var doc = db.get("1234")

# Query documents
var docs = db.query()
    .where("type", "==", "example")
    .sort("timestamp", ascending = false)
    .limit(10)
    .list()

# Close the database
db.close()
```

## Collections

NJSDB supports multiple collections (tables) in the same database. Each collection is stored in a separate SQLite table, providing data isolation.

```nim
# Select a collection before any operations
db.collection("users")
db.put(%*{ "id": "u1", "name": "Alice" })

# Switch to another collection
db.collection("orders")
db.put(%*{ "id": "o1", "total": 100 })

# Method chaining is supported
db.collection("products")
   .query()
   .where("price", ">", 50)
   .list()
```

## CRUD Operations

### Create / Update

```nim
db.collection("documents")

# Insert or replace a document
db.put(%*{
    "id": "doc1",
    "name": "Test Document",
    "value": 42
})

# Auto-generate ID (if not provided)
db.put(%*{ "name": "Auto ID Document" })  # ID will be auto-generated

# Update with merge (partial update)
db.put(%*{
    "id": "doc1",
    "name": "Updated Name"
}, merge = true)

# Upsert: Insert or update document (full replace)
db.upsert(%*{
    "id": "doc1",
    "timestamp": 123456,
    "type": "example",
    "text": "Updated text!"
})

# Upsert with merge (partial update if exists)
db.upsert(%*{
    "id": "doc1",
    "text": "Only update this field"
}, merge = true)
```

### Read

```nim
db.collection("documents")

# Get document by ID
let doc = db.get("doc1")

# Query with filters
let results = db.query()
    .where("status", "==", "active")
    .where("age", ">=", 18)
    .list()

# Get single result
let first = db.query()
    .where("type", "==", "example")
    .get()

# Count documents
let count = db.query().where("status", "==", "active").count()

# Check if document exists
let exists = db.get("doc1") != nil
```

### Delete

```nim
db.collection("documents")

# Delete by ID
let deleted = db.delete("doc1")  # Returns true if deleted

# Delete by query
let deletedCount = db.query()
    .where("status", "==", "inactive")
    .delete()
```

## Query Operations

### Basic Queries

```nim
db.collection("documents")

# Chain multiple where clauses
db.query()
    .where("status", "==", "active")
    .where("age", ">=", 18)
    .list()

# Available operators: ==, !=, <, <=, >, >=
db.query().where("score", ">", 100).list()
db.query().where("name", "!=", "").list()
```

### MongoDB-style Filters

```nim
db.collection("documents")

# Using filter() with JSON objects
let filter = %*{
    "status": "active",
    "age": { "$gte": 18 }
}
db.query().filter(filter).list()

# $eq, $ne, $gt, $gte, $lt, $lte
let filter = %*{
    "age": { "$gte": 18, "$lte": 65 }
}

# $in operator
let filter = %*{
    "type": { "$in": ["A", "B", "C"] }
}

# $nin operator (not in)
let filter = %*{
    "status": { "$nin": ["deleted", "archived"] }
}
```

### Logical Operators

```nim
db.collection("documents")

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

# $nor operator
let filter = %*{
    "$nor": [
        { "status": "deleted" },
        { "status": "archived" }
    ]
}

# $not operator
let filter = %*{
    "age": { "$not": { "$lt": 18 } }  # age >= 18
}

# Mixed operators
let filter = %*{
    "status": "active",
    "$or": [
        { "type": "A" },
        { "priority": { "$gt": 5 } }
    ]
}
```

### Array Operators

```nim
db.collection("documents")

# $all - Array contains all specified values
let filter = %*{
    "tags": { "$all": ["important", "urgent"] }
}
db.query().filter(filter).list()

# $size - Array has specific length
let filter = %*{
    "tags": { "$size": 3 }
}
```

### Existence and Type Operators

```nim
db.collection("documents")

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
```

### Nested Field Queries

```nim
db.collection("documents")

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

## Sorting, Limiting, and Pagination

```nim
db.collection("documents")

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
db.collection("documents")

# Include only specific fields
let projection = %*{ "name": 1, "email": 1 }
db.query().project(projection).list()

# Exclude specific fields
let projection = %*{ "password": 0, "secretKey": 0 }
db.query().project(projection).list()

# Note: Cannot mix include and exclude (except _id can always be excluded)
```

## Update Operators

```nim
db.collection("documents")

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

# Update single document by ID
db.updateOne("doc1", %*{
    "$set": { "status": "completed" }
})
```

## Aggregation

### Basic Aggregation

```nim
db.collection("documents")

# Extended aggregation with multiple operators
let aggResult = db.aggregate("category", %*{
    "$sum": "amount",
    "$avg": "price",
    "$min": "stock",
    "$max": "stock"
})

# Access aggregation results
for agg in aggResult:
    echo "Category: ", agg.groupId
    echo "Count: ", agg.count
    echo "Total: ", agg.sum
    echo "Average: ", agg.avg
    echo "Min: ", agg.min
    echo "Max: ", agg.max

# Aggregate with filter
let filter = %*{ "status": "active" }
let aggResult2 = db.aggregate("category", %*{ "$sum": "amount" }, filter)
```

### Aggregation Pipeline

MongoDB-style aggregation pipeline with multiple stages:

```nim
db.collection("documents")

# Basic aggregation pipeline: match and group
let pipelineResult = db.aggregate(@[
    %*{ "$match": { "status": "completed" } },
    %*{ "$group": { "_id": "$customerId", "total": { "$sum": "$amount" } } }
])

for doc in pipelineResult.data:
    echo "Customer: ", doc["_id"].getStr
    echo "Total: ", doc["total"].getFloat

# Multiple aggregation operators
let pipelineResult2 = db.aggregate(@[
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

# Pipeline stages: $match, $group, $sort, $limit, $skip, $project, $count
let pipelineResult3 = db.aggregate(@[
    %*{ "$match": { "status": "active" } },
    %*{ "$group": { "_id": "$category", "count": { "$sum": 1 } } },
    %*{ "$sort": { "count": -1 } },
    %*{ "$skip": 5 },
    %*{ "$limit": 10 }
])

# $project stage
let pipelineResult4 = db.aggregate(@[
    %*{ "$match": { "status": "active" } },
    %*{ "$project": { "name": 1, "email": 1 } }
])

# $count stage
let pipelineResult5 = db.aggregate(@[
    %*{ "$match": { "status": "active" } },
    %*{ "$count": "activeCount" }
])
```

## Bulk Operations

```nim
db.collection("documents")

# Insert multiple documents efficiently
let docs = @[
    %*{ "name": "Item 1", "value": 10 },
    %*{ "name": "Item 2", "value": 20 },
    %*{ "name": "Item 3", "value": 30 }
]
let inserted = db.insertMany(docs)
```

## Transactions

```nim
db.collection("documents")

# Execute operations in a transaction
db.withTransaction(proc() {.gcsafe.} =
    db.put(%*{ "id": "1", "value": 100 })
    db.put(%*{ "id": "2", "value": 200 })
    # Both operations succeed or both fail
)
```

## Utility Methods

```nim
db.collection("documents")

# Get distinct values for a field
let categories = db.query().distinctValues("category")

# Query explain - analyze query performance
let plan = db.query()
    .where("field", "==", "value")
    .explain()
# Returns query plan with operation details

# Memory-efficient iteration
for doc in db.query().where("status", "==", "active").list():
    echo doc["name"].getStr
```

## Error Handling

NJSDB defines several exception types:

```nim
db.collection("documents")

try:
    db.put(%*{ "id": "test" })
except ValidationError:
    echo "Invalid input"
except DocumentError:
    echo "Document operation failed"
except NJSDBError:
    echo "General database error"
```

## API Reference

### NJSDB Class

| Method | Description |
|--------|-------------|
| `open(filename)` | Open database connection |
| `close()` | Close database connection |
| `collection(name)` | Select collection for operations |
| `put(doc, merge=false)` | Insert or replace document |
| `get(id)` | Get document by ID |
| `delete(id)` | Delete document by ID |
| `upsert(doc)` | Update or insert document (full replace) |
| `upsert(doc, merge)` | Update or insert document (with merge option) |
| `updateOne(id, updates)` | Update single document |
| `insertMany(docs)` | Batch insert documents |
| `query()` | Start a query builder |
| `aggregate(groupField, aggregations, filter)` | Perform aggregation |
| `aggregate(pipeline)` | Aggregation pipeline |
| `withTransaction(proc)` | Execute in transaction |

### NJSDBQuery Class

| Method | Description |
|--------|-------------|
| `where(field, op, value)` | Add filter condition |
| `filter(json)` | Add MongoDB-style filter |
| `sort(field, asc, isNum)` | Set sort order |
| `limit(n)` | Limit results |
| `offset(n)` | Skip results |
| `project(json)` | Select fields |
| `list()` | Execute query, return results |
| `get()` | Get first result |
| `count()` | Count matching documents |
| `delete()` | Delete matching documents |
| `update(updates)` | Update matching documents |
| `explain()` | Get query plan |
| `distinctValues(field)` | Get unique values |

## Multi-threading

NJSDB supports multi-threading. Each thread should create its own `NJSDB` instance with its own database connection. See the `examples/` directory for complete examples.

### Basic Multi-threading Pattern

```nim
import std/[os, json, times]
import njsdb

# Each thread creates its own database connection
proc workerThread(threadId: int) {.thread.} =
    var db = NJSDB()
    db.open("database.db")
    db.collection("documents")

    # Insert documents
    for i in 0..<100:
        db.put(%*{
            "id": "thread" & $threadId & "_doc" & $i,
            "threadId": threadId,
            "sequence": i
        })

    # Query documents
    let results = db.query()
        .where("threadId", "==", threadId)
        .list()

    echo "Thread ", threadId, " processed ", results.len, " documents"
    db.close()

# Main thread
var threads: array[4, Thread[int]]
for i in 0..<4:
    createThread(threads[i], workerThread, i)

for i in 0..<4:
    joinThread(threads[i])
```

### Key Points

- **Each thread needs its own connection**: Create a separate `NJSDB` instance in each thread
- **SQLite handles concurrency**: Multiple threads can read concurrently, writes are serialized
- **Use transactions for batch operations**: Wrap multiple operations in `withTransaction()` for atomicity
- **Thread pool pattern**: Use a work queue with multiple worker threads for high-throughput scenarios

See `examples/multithread_example.nim` and `examples/multithread_shared_example.nim` for complete working examples.

## License

MIT
