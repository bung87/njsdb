# SimpleDB Features Roadmap

## Current Implementation Status

### ✅ Already Implemented

| Feature | Implementation | Notes |
|---------|---------------|-------|
| **Basic CRUD** | `put()`, `get()`, `remove()` | Full document storage/retrieval |
| **Query Builder** | `.where()`, `.sort()`, `.limit()`, `.offset()` | Chainable API |
| **Comparison Operators** | `==`, `!=`, `<`, `<=`, `>`, `>=` | Both method and filter styles |
| **MongoDB-style Filter** | `.filter(JsonNode)` | JSON-based query syntax |
| **Filter Operators** | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in` | Via filter() method |
| **Merge Updates** | `put(doc, merge = true)` | Partial document updates |
| **Batch Operations** | `.batch()` | Transaction wrapper |
| **Count** | `.count()` | Document counting |
| **Distinct Values** | `.distinctValues(field)` | Unique field values |
| **Update with $set** | `.update(%*{"$set": {...}})` | Partial updates via query |
| **Auto-generated IDs** | `genOid()` | Automatic ID generation |
| **Auto-indexing** | Dynamic index creation | Based on query patterns |
| **Iterator** | `iterator list()` | Memory-efficient iteration |
| **Basic Aggregation** | `aggregateCount()` | Group by and count |

---

## Proposed Features

### 🔥 High Priority

#### 1. Nested Field Queries (Dot Notation)
Query nested JSON fields using dot notation like MongoDB.

```nim
# Current limitation: Cannot query nested fields easily
# Proposed:
db.query().where("user.name", "==", "John")
db.query().filter(%*{ "address.zipcode": "10001" })
```

**Implementation Notes:**
- SQLite's `json_extract(_json, '$.user.name')` supports this
- Convert dot notation to JSON path in query builder
- Affects: `where()`, `filter()`, `sort()`, `distinctValues()`

#### 2. Logical Operators (`$or`, `$nor`, `$not`)
Complex conditional queries beyond implicit AND.

```nim
# Current: Only implicit AND via chained .where()
# Proposed:
db.query().filter(%*{
  "$or": [
    { "status": "active" },
    { "priority": { "$gt": 5 } }
  ]
})

# Mixed AND/OR
db.query().filter(%*{
  "$and": [
    { "type": "user" },
    { "$or": [
      { "status": "active" },
      { "status": "pending" }
    ]}
  ]
})
```

**Implementation Notes:**
- Requires SQL OR operator support in query builder
- Need to handle nested logical operators
- Consider query optimization (flatten nested ANDs)

#### 3. Array Operators
Query documents based on array field contents.

```nim
# $all - Array contains all specified values
db.query().filter(%*{
  "tags": { "$all": ["important", "urgent"] }
})

# $elemMatch - Array element matches criteria
db.query().filter(%*{
  "comments": {
    "$elemMatch": {
      "author": "John",
      "rating": { "$gte": 4 }
    }
  }
})

# $size - Array has specific length
db.query().filter(%*{
  "tags": { "$size": 3 }
})
```

**Implementation Notes:**
- `$all`: Use `json_each` table-valued function or multiple `json_array_contains`
- `$elemMatch`: Complex - may need subquery or custom function
- `$size`: `json_array_length(json_extract(_json, '$.field'))`

#### 4. Existence Check (`$exists`)
Query for field presence or absence.

```nim
# Field exists
db.query().filter(%*{
  "deletedAt": { "$exists": false }
})

# Field does not exist
db.query().filter(%*{
  "archived": { "$exists": false }
})
```

**Implementation Notes:**
- SQLite: `json_type(json_extract(_json, '$.field')) IS NOT NULL`
- Need to distinguish between null and undefined

#### 5. Projection (Field Selection)
Return only specific fields instead of full documents.

```nim
# Include only specific fields
db.query()
  .where("type", "==", "user")
  .project(%*{ "name": 1, "email": 1 })

# Exclude specific fields
db.query()
  .where("type", "==", "user")
  .project(%*{ "password": 0, "secretKey": 0 })
```

**Implementation Notes:**
- Instead of `SELECT _json`, use multiple `json_extract` calls
- Return partial JsonNode with only selected fields
- Consider performance: extracting fewer fields = less data transfer

#### 6. Upsert
Update or insert if document doesn't exist.

```nim
# Current: Separate check needed
# Proposed:
db.update(id, updates, upsert = true)

# Or via query
db.query()
  .where("externalId", "==", "ext-123")
  .update(updates, upsert = true)
```

**Implementation Notes:**
- SQLite: `INSERT ... ON CONFLICT(...) DO UPDATE`
- Or use existing merge logic with a flag

---

### ⚡ Medium Priority

#### 7. Regex Matching (`$regex`)
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
- Consider: `REGEXP` operator with custom function

#### 8. Array Update Operators
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
- Consider atomicity and performance

#### 9. Increment/Decrement (`$inc`, `$mul`)
Atomic numeric operations.

```nim
# Increment
db.update(id, %*{
  "$inc": { "views": 1, "likes": 1 }
})

# Decrement
db.update(id, %*{
  "$inc": { "stock": -5 }
})

# Multiply
db.update(id, %*{
  "$mul": { "price": 1.1 }  # 10% increase
})
```

**Implementation Notes:**
- SQLite: `json_set` with calculated value
- Must handle non-existent fields (default to 0)
- Atomic operation within UPDATE statement

#### 10. Field Operations (`$unset`, `$rename`)
Remove or rename fields.

```nim
# Remove field
db.update(id, %*{
  "$unset": { "tempField": "" }
})

# Rename field
db.update(id, %*{
  "$rename": { "oldName": "newName" }
})
```

**Implementation Notes:**
- `$unset`: Use `json_remove()` in SQLite
- `$rename`: Combine `json_insert` with `json_remove`

#### 11. Type Checking (`$type`)
Query by JSON value type.

```nim
db.query().filter(%*{
  "score": { "$type": "number" }
})

# Supported types: "string", "number", "boolean", "array", "object", "null"
```

**Implementation Notes:**
- SQLite: `json_type(json_extract(_json, '$.field'))`
- Returns: 'null', 'true', 'false', 'integer', 'real', 'text', 'array', 'object'

#### 12. Query Explain
Analyze query performance.

```nim
let plan = db.query()
  .where("field", "==", "value")
  .explain()
# Returns query plan for optimization
```

**Implementation Notes:**
- SQLite: `EXPLAIN QUERY PLAN SELECT ...`
- Helpful for debugging index usage

---

### 📊 Aggregation Enhancements

#### 13. Extended Aggregation Operators
More aggregation functions.

```nim
# Sum
db.aggregate()
  .match(%*{ "status": "active" })
  .group("category", %*{
    "totalSales": { "$sum": "$amount" },
    "avgPrice": { "$avg": "$price" },
    "minPrice": { "$min": "$price" },
    "maxPrice": { "$max": "$price" },
    "firstOrder": { "$first": "$orderDate" },
    "lastOrder": { "$last": "$orderDate" }
  })
```

**Implementation Notes:**
- Leverage SQL aggregate functions
- Group by multiple fields support

#### 14. Aggregation Pipeline Stages
Chainable aggregation stages.

```nim
db.aggregate()
  .match(%*{ "status": "active" })           # Filter
  .project(%*{ "category": 1, "amount": 1 }) # Select fields
  .group("category", %*{"total": { "$sum": "$amount" }})
  .sort("total", ascending = false)
  .limit(10)
```

---

### 🔍 Search & Indexing

#### 15. Full-Text Search (`$text`)
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
- Consider: `CREATE VIRTUAL TABLE ... USING fts5(...)`

#### 16. Geospatial Queries
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
- Store lat/lng as separate indexed columns or use R*Tree

---

### 💾 Data Management

#### 17. Transactions
Multi-operation ACID transactions.

```nim
db.transaction:
  db.put(doc1)
  db.put(doc2)
  db.remove(id)
  # All succeed or all rollback
```

**Implementation Notes:**
- SQLite has excellent transaction support
- Current `.batch()` provides basic transaction wrapper
- Could extend with savepoints, rollback, etc.

#### 18. TTL (Time-To-Live)
Auto-expire documents.

```nim
# Set expiration
db.put(%*{
  "data": "value",
  "expireAt": db.ttl(hours = 24)
})

# Or
db.put(doc, ttl = 3600)  # seconds

# Background cleanup (or lazy deletion)
db.cleanupExpired()
```

**Implementation Notes:**
- Store expiration timestamp
- Cleanup via background task or query-time filtering
- Index on expireAt field for performance

#### 19. Cursor-Based Pagination
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

**Implementation Notes:**
- Uses indexed field for positioning
- More efficient than OFFSET for large datasets
- Requires sort field to be unique or compound

#### 20. Bulk Operations
Efficient batch operations.

```nim
# Bulk insert (faster than individual puts)
db.bulkInsert(docs)  # seq[JsonNode]

# Bulk update
db.bulkUpdate(
  queries = @[query1, query2],
  updates = @[update1, update2]
)

# Bulk delete
db.bulkDelete(ids)
```

**Implementation Notes:**
- Use single transaction with prepared statements
- Batch bindings for better performance

---

## Implementation Priority Matrix

| Feature | User Impact | Implementation Complexity | SQLite Support |
|---------|-------------|--------------------------|----------------|
| Dot Notation | High | Low | ✅ json_extract |
| $or / $and | High | Medium | ✅ SQL operators |
| Array Operators | High | Medium | ✅ json_each, json_array_length |
| $exists | Medium | Low | ✅ json_type |
| Projection | Medium | Low | ✅ json_extract |
| Upsert | Medium | Low | ✅ ON CONFLICT |
| $regex | Medium | Low | ⚠️ Needs extension |
| Array Updates | Medium | High | ⚠️ Complex JSON manipulation |
| $inc / $mul | Medium | Low | ✅ json_set |
| $unset / $rename | Low | Low | ✅ json_remove |
| $type | Low | Low | ✅ json_type |
| Query Explain | Low | Low | ✅ EXPLAIN |
| Aggregation | Medium | Medium | ✅ SQL aggregates |
| Full-Text Search | Medium | Medium | ⚠️ FTS5 extension |
| Geospatial | Low | High | ⚠️ R*Tree extension |
| Transactions | Medium | Low | ✅ Native support |
| TTL | Low | Medium | ✅ User-space |
| Cursor Pagination | Medium | Low | ✅ Indexed queries |
| Bulk Operations | Medium | Low | ✅ Batch processing |

---

## Notes for Discussion

1. **SQLite Extension Dependencies**: Some features require SQLite extensions (REGEXP, FTS5, R*Tree). Should these be optional features?

2. **Performance vs Compatibility**: More MongoDB-like features may require complex SQL. How to balance?

3. **API Consistency**: Should we stick closer to MongoDB syntax or optimize for Nim idioms?

4. **Index Management**: Current auto-indexing is implicit. Should users have explicit index control?

5. **Schema Validation**: Should we add JSON Schema validation for document structure?

6. **Migrations**: How to handle schema/index changes in production?
