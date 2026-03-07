# NJSDB Features Roadmap

## Current Implementation Status

### ✅ Already Implemented

| Feature | Implementation | Notes |
|---------|---------------|-------|
| **Basic CRUD** | `put()`, `get()`, `remove()` | Full document storage/retrieval |
| **Query Builder** | `.where()`, `.sort()`, `.limit()`, `.offset()` | Chainable API |
| **Comparison Operators** | `==`, `!=`, `<`, `<=`, `>`, `>=` | Both method and filter styles |
| **MongoDB-style Filter** | `.filter(JsonNode)` | JSON-based query syntax |
| **Filter Operators** | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in` | Via filter() method |
| **Nested Field Queries** | `where("user.name", "==", "John")` | Dot notation support |
| **Logical Operators** | `$or`, `$and` | Complex conditional queries |
| **Array Operators** | `$all`, `$size` | Array field queries |
| **Existence Check** | `$exists` | Field presence/absence |
| **Type Checking** | `$type` | Query by JSON value type |
| **Merge Updates** | `put(doc, merge = true)` | Partial document updates |
| **Batch Operations** | `.batch()` | Transaction wrapper |
| **Count** | `.count()` | Document counting |
| **Distinct Values** | `.distinctValues(field)` | Unique field values |
| **Update with $set** | `.update(%*{"$set": {...}})` | Partial updates via query |
| **Update with $unset** | `.update(%*{"$unset": {...}})` | Remove fields via query |
| **Update with $rename** | `.update(%*{"$rename": {...}})` | Rename fields via query |
| **Update with $inc** | `.update(%*{"$inc": {...}})` | Increment numeric fields |
| **Update with $mul** | `.update(%*{"$mul": {...}})` | Multiply numeric fields |
| **Auto-generated IDs** | `genOid()` | Automatic ID generation |
| **Auto-indexing** | Dynamic index creation | Based on query patterns |
| **Iterator** | `iterator list()` | Memory-efficient iteration |
| **Query Explain** | `.explain()` | Query plan analysis |
| **Projection** | `.project()` | Field selection (include/exclude) |
| **Upsert** | `upsert(doc)` | Update or insert document |
| **Bulk Insert** | `bulkInsert(docs)` | Efficient batch insert |
| **Bulk Delete** | `bulkDelete(ids)` | Efficient batch delete |
| **Basic Aggregation** | `aggregateCount()` | Group by and count |
| **Extended Aggregation** | `aggregate()` | Sum, avg, min, max |
| **Aggregation Pipeline** | `aggregate(pipeline)` | MongoDB-style pipeline |
| **Pipeline Stages** | `$match`, `$group`, `$sort`, `$limit`, `$skip`, `$project`, `$count` | Full pipeline support |
| **Collections** | `collection(name)` | Multi-table support |

---

## Usage Examples

### Basic Operations

```nim
import njsdb

var db = NJSDB()
db.open(":memory:")
db.collection("users")

# Insert
db.put(%*{ "id": "user1", "name": "Alice", "age": 30 })

# Query
let user = db.get("user1")
let adults = db.query().where("age", ">=", 18).list()

# Update
db.updateOne("user1", %*{ "$set": { "age": 31 } })

# Delete
db.removeOne("user1")
```

### Advanced Queries

```nim
# Nested fields
db.query().where("address.city", "==", "New York")

# Logical operators
db.query().filter(%*{
  "$or": [
    { "status": "active" },
    { "priority": { "$gt": 5 } }
  ]
})

# Array operators
db.query().filter(%*{
  "tags": { "$all": ["important", "urgent"] }
})
db.query().filter(%*{
  "tags": { "$size": 3 }
})

# Exists check
db.query().filter(%*{
  "archived": { "$exists": false }
})

# Projection
db.query()
  .where("type", "==", "user")
  .project(%*{ "name": 1, "email": 1 })
  .list()
```

### Aggregation Pipeline

```nim
# Group and sum
let result = db.aggregate(@[
  %*{ "$match": { "status": "completed" } },
  %*{ "$group": { "_id": "$customerId", "total": { "$sum": "$amount" } } },
  %*{ "$sort": { "total": -1 } },
  %*{ "$limit": 10 }
])

# Count distinct
let countResult = db.aggregate(@[
  %*{ "$match": { "activityId": { "$ne": "" } } },
  %*{ "$group": { "_id": "$activityId" } },
  %*{ "$count": "distinctCount" }
])
```

### Batch Operations

```nim
# Batch insert
let docs = @[
  %*{ "id": "1", "name": "A" },
  %*{ "id": "2", "name": "B" },
  %*{ "id": "3", "name": "C" }
]
db.bulkInsert(docs)

# Batch delete
db.bulkDelete(@["1", "2", "3"])

# Transaction batch
let dbPtr = addr db
db.batch(proc() {.gcsafe.} =
  dbPtr[].put(%*{ "id": "1", "value": 100 })
  dbPtr[].put(%*{ "id": "2", "value": 200 })
)
```

---

## Proposed Features

### 🔥 High Priority

#### 1. Regex Matching (`$regex`)
Pattern matching for string fields.

```nim
db.query().filter(%*{
  "name": { "$regex": "^Jo.*n$" }
})

# Case-insensitive
db.query().filter(%*{
  "email": { "$regex": "@gmail\\.com$", "$options": "i" }
})
```

**Implementation Notes:**
- SQLite doesn't have native regex by default
- Need to load REGEXP extension or use LIKE/GLOB as fallback

#### 2. Array Update Operators
Modify arrays without fetching entire document.

```nim
# $push - Add to array
db.query().where("id", "==", "doc1").update(%*{
  "$push": { "tags": "new-tag" }
})

# $pull - Remove from array
db.query().where("id", "==", "doc1").update(%*{
  "$pull": { "tags": "old-tag" }
})

# $addToSet - Add if not exists
db.query().where("id", "==", "doc1").update(%*{
  "$addToSet": { "categories": "unique-value" }
})

# $pop - Remove first/last element
db.query().where("id", "==", "doc1").update(%*{
  "$pop": { "queue": 1 }  # 1 = last, -1 = first
})
```

**Implementation Notes:**
- Complex: Requires fetching, modifying, and saving JSON
- SQLite 3.38+ has `json_insert`, `json_replace`, `json_remove`

#### 3. Aggregation Enhancements
More pipeline stages and operators.

```nim
# $lookup (join)
db.aggregate(@[
  %*{ "$match": { "status": "active" } },
  %*{ "$lookup": {
    "from": "users",
    "localField": "userId",
    "foreignField": "_id",
    "as": "user"
  }}
])

# $unwind
db.aggregate(@[
  %*{ "$unwind": "$tags" }
])

# Additional operators: $first, $last, $push, $addToSet
db.aggregate(@[
  %*{ "$group": {
    "_id": "$category",
    "firstItem": { "$first": "$name" },
    "allTags": { "$push": "$tags" }
  }}
])
```

---

### ⚡ Medium Priority

#### 4. Full-Text Search (`$text`)
Text search using SQLite FTS5.

```nim
# Requires FTS5 virtual table
db.query().filter(%*{
  "$text": { "$search": "hello world" }
})

# Phrase search
db.query().filter(%*{
  "$text": { "$search": "\"exact phrase\"" }
})
```

**Implementation Notes:**
- Requires FTS5 extension (included in most SQLite builds)
- Need separate virtual table for full-text index

#### 5. Geospatial Queries
Location-based queries using R*Tree.

```nim
# Find nearby
db.query().filter(%*{
  "location": {
    "$near": {
      "lat": 40.7128,
      "lng": -74.0060,
      "maxDist": 1000  # meters
    }
  }
})

# Within bounding box
db.query().filter(%*{
  "location": {
    "$within": {
      "box": [[40.7, -74.1], [40.8, -73.9]]
    }
  }
})
```

**Implementation Notes:**
- SQLite R*Tree extension for spatial indexing

#### 6. TTL (Time-To-Live)
Auto-expire documents.

```nim
# Set expiration
db.put(%*{
  "data": "value",
  "expireAt": db.ttl(hours = 24)
})

# Or
db.put(doc, ttl = 3600)  # seconds

# Background cleanup
db.cleanupExpired()
```

#### 7. Cursor-Based Pagination
More efficient for large datasets.

```nim
# Instead of offset (slow for large datasets)
let page1 = db.query()
  .where("createdAt", ">", lastTimestamp)
  .sort("createdAt", ascending = true)
  .limit(100)
  .list()

# Get next cursor
let lastDoc = page1[^1]
let page2 = db.query()
  .where("createdAt", ">", lastDoc["createdAt"].getFloat)
  .sort("createdAt", ascending = true)
  .limit(100)
  .list()
```

---

### 📊 Aggregation Pipeline Stages Status

| Stage | Status | Notes |
|-------|--------|-------|
| `$match` | ✅ Implemented | Filter documents |
| `$group` | ✅ Implemented | Group by field |
| `$sort` | ✅ Implemented | Sort results |
| `$limit` | ✅ Implemented | Limit results |
| `$skip` | ✅ Implemented | Skip documents |
| `$project` | ✅ Implemented | Field selection |
| `$count` | ✅ Implemented | Count documents |
| `$lookup` | ❌ Not implemented | Join collections |
| `$unwind` | ❌ Not implemented | Unwind arrays |
| `$facet` | ❌ Not implemented | Multi-stage aggregation |

### 📊 Aggregation Operators Status

| Operator | Status | Notes |
|----------|--------|-------|
| `$sum` | ✅ Implemented | Sum values |
| `$avg` | ✅ Implemented | Average values |
| `$min` | ✅ Implemented | Minimum value |
| `$max` | ✅ Implemented | Maximum value |
| `$first` | ❌ Not implemented | First value in group |
| `$last` | ❌ Not implemented | Last value in group |
| `$push` | ❌ Not implemented | Add to array |
| `$addToSet` | ❌ Not implemented | Add unique to array |

### 📊 Query Operators Status

| Operator | Status | Notes |
|----------|--------|-------|
| `$eq` | ✅ Implemented | Equal |
| `$ne` | ✅ Implemented | Not equal |
| `$gt` | ✅ Implemented | Greater than |
| `$gte` | ✅ Implemented | Greater than or equal |
| `$lt` | ✅ Implemented | Less than |
| `$lte` | ✅ Implemented | Less than or equal |
| `$in` | ✅ Implemented | In array |
| `$nin` | ❌ Not implemented | Not in array |
| `$exists` | ✅ Implemented | Field exists |
| `$type` | ✅ Implemented | Type check |
| `$regex` | ❌ Not implemented | Pattern matching |
| `$all` | ✅ Implemented | Array contains all |
| `$size` | ✅ Implemented | Array size |
| `$or` | ✅ Implemented | Logical OR |
| `$and` | ✅ Implemented | Logical AND |
| `$not` | ❌ Not implemented | Logical NOT |
| `$nor` | ❌ Not implemented | Logical NOR |

### 📊 Update Operators Status

| Operator | Status | Notes |
|----------|--------|-------|
| `$set` | ✅ Implemented | Set field value |
| `$unset` | ✅ Implemented | Remove field |
| `$inc` | ✅ Implemented | Increment field |
| `$mul` | ✅ Implemented | Multiply field |
| `$rename` | ✅ Implemented | Rename field |
| `$push` | ❌ Not implemented | Add to array |
| `$pull` | ❌ Not implemented | Remove from array |
| `$addToSet` | ❌ Not implemented | Add unique to array |
| `$pop` | ❌ Not implemented | Remove from array ends |
| `$slice` | ❌ Not implemented | Slice array |

---

## Implementation Priority Matrix

| Feature | User Impact | Implementation Complexity | SQLite Support |
|---------|-------------|--------------------------|----------------|
| $regex | Medium | Low | ⚠️ Needs extension |
| Array Updates | Medium | High | ⚠️ Complex JSON manipulation |
| Aggregation $lookup | High | High | ⚠️ Requires JOIN logic |
| Aggregation $unwind | Medium | Medium | ⚠️ Complex transformation |
| Full-Text Search | Medium | Medium | ⚠️ FTS5 extension |
| Geospatial | Low | High | ⚠️ R*Tree extension |
| TTL | Low | Medium | ✅ User-space |
| Cursor Pagination | Medium | Low | ✅ Indexed queries |

---

## Notes for Discussion

1. **SQLite Extension Dependencies**: Some features require SQLite extensions (REGEXP, FTS5, R*Tree). Should these be optional features?

2. **Performance vs Compatibility**: More MongoDB-like features may require complex SQL. How to balance?

3. **API Consistency**: Should we stick closer to MongoDB syntax or optimize for Nim idioms?

4. **Index Management**: Current auto-indexing is implicit. Should users have explicit index control?

5. **Schema Validation**: Should we add JSON Schema validation for document structure?

6. **Migrations**: How to handle schema/index changes in production?
