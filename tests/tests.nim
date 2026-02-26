import unittest
import std/[json, times, random, os]
import simpledb

suite "SimpleDB Basic Operations":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")

  teardown:
    db.close()

  test "Open and close database":
    check db != nil

  test "Put and get document":
    let doc = %*{
      "id": "test-1",
      "name": "Test Document",
      "value": 42
    }
    db.put(doc)
    
    let retrieved = db.get("test-1")
    check retrieved != nil
    check retrieved["name"].getStr == "Test Document"
    check retrieved["value"].getInt == 42

  test "Put document without ID generates ID":
    let doc = %*{ "name": "No ID" }
    db.put(doc)
    check doc["id"].getStr.len > 0

  test "Merge update document":
    db.put(%*{
      "id": "merge-test",
      "field1": "original",
      "field2": "keep"
    })
    
    db.put(%*{
      "id": "merge-test",
      "field1": "updated"
    }, merge = true)
    
    let retrieved = db.get("merge-test")
    check retrieved["field1"].getStr == "updated"
    check retrieved["field2"].getStr == "keep"

  test "Replace document":
    db.put(%*{
      "id": "replace-test",
      "field1": "original",
      "field2": "original"
    })
    
    db.put(%*{
      "id": "replace-test",
      "field3": "new"
    })
    
    let retrieved = db.get("replace-test")
    check retrieved["field3"].getStr == "new"
    check retrieved.hasKey("field1") == false

  test "Get non-existent document returns nil":
    let retrieved = db.get("non-existent")
    check retrieved == nil

  test "Remove document by ID":
    db.put(%*{ "id": "to-delete", "data": "value" })
    check db.get("to-delete") != nil
    
    let removed = db.remove("to-delete")
    check removed == true
    check db.get("to-delete") == nil

  test "Remove non-existent document returns false":
    let removed = db.remove("non-existent")
    check removed == false


suite "SimpleDB Query Operations":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    # Seed test data
    for i in 0 ..< 10:
      db.put(%*{
        "id": "doc-" & $i,
        "type": "test",
        "index": i,
        "category": if i mod 2 == 0: "even" else: "odd",
        "score": i.float * 10.0
      })

  teardown:
    db.close()

  test "Query with equality filter":
    let docs = db.query()
      .where("type", "==", "test")
      .list()
    check docs.len == 10

  test "Query with greater than filter":
    let docs = db.query()
      .where("index", ">", 5)
      .list()
    check docs.len == 4
    for doc in docs:
      check doc["index"].getInt > 5

  test "Query with less than filter":
    let docs = db.query()
      .where("index", "<", 3)
      .list()
    check docs.len == 3

  test "Query with range filter":
    let docs = db.query()
      .where("index", ">=", 3)
      .where("index", "<=", 6)
      .list()
    check docs.len == 4

  test "Query with limit":
    let docs = db.query()
      .where("type", "==", "test")
      .limit(5)
      .list()
    check docs.len == 5

  test "Query with offset":
    let docs = db.query()
      .where("type", "==", "test")
      .sort("index", ascending = true)
      .offset(5)
      .list()
    check docs.len == 5
    check docs[0]["index"].getInt == 5

  test "Query with sort ascending":
    let docs = db.query()
      .where("type", "==", "test")
      .sort("index", ascending = true)
      .list()
    check docs[0]["index"].getInt == 0
    check docs[9]["index"].getInt == 9

  test "Query with sort descending":
    let docs = db.query()
      .where("type", "==", "test")
      .sort("index", ascending = false)
      .list()
    check docs[0]["index"].getInt == 9
    check docs[9]["index"].getInt == 0

  test "Query get single document":
    let doc = db.query()
      .where("id", "==", "doc-5")
      .get()
    check doc != nil
    check doc["index"].getInt == 5

  test "Query get non-existent returns nil":
    let doc = db.query()
      .where("id", "==", "non-existent")
      .get()
    check doc == nil

  test "Query iterator":
    var count = 0
    for doc in db.query().where("type", "==", "test").list():
      count += 1
      check doc["type"].getStr == "test"
    check count == 10


suite "SimpleDB MongoDB-style Filter":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    # Seed test data
    for i in 0 ..< 10:
      db.put(%*{
        "id": "doc-" & $i,
        "type": "test",
        "status": if i mod 3 == 0: "active" elif i mod 3 == 1: "pending" else: "inactive",
        "priority": i,
        "score": i.float * 10.0
      })

  teardown:
    db.close()

  test "Filter with $in operator":
    let filter = %*{ "status": { "$in": ["active", "pending"] } }
    let docs = db.query()
      .where("type", "==", "test")
      .filter(filter)
      .list()
    check docs.len == 7  # indices 0,1,3,4,6,7,9

  test "Filter with $eq operator":
    let filter = %*{ "status": { "$eq": "active" } }
    let docs = db.query()
      .where("type", "==", "test")
      .filter(filter)
      .list()
    check docs.len == 4  # indices 0,3,6,9

  test "Filter with $gt operator":
    let filter = %*{ "priority": { "$gt": 5 } }
    let docs = db.query()
      .where("type", "==", "test")
      .filter(filter)
      .list()
    check docs.len == 4  # indices 6,7,8,9

  test "Filter with $gte operator":
    let filter = %*{ "priority": { "$gte": 5 } }
    let docs = db.query()
      .where("type", "==", "test")
      .filter(filter)
      .list()
    check docs.len == 5  # indices 5,6,7,8,9

  test "Filter with $lt operator":
    let filter = %*{ "priority": { "$lt": 3 } }
    let docs = db.query()
      .where("type", "==", "test")
      .filter(filter)
      .list()
    check docs.len == 3  # indices 0,1,2

  test "Filter with $lte operator":
    let filter = %*{ "priority": { "$lte": 3 } }
    let docs = db.query()
      .where("type", "==", "test")
      .filter(filter)
      .list()
    check docs.len == 4  # indices 0,1,2,3

  test "Filter with $ne operator":
    let filter = %*{ "status": { "$ne": "active" } }
    let docs = db.query()
      .where("type", "==", "test")
      .filter(filter)
      .list()
    check docs.len == 6  # not indices 0,3,6,9

  test "Filter with simple string value (implicit $eq)":
    let filter = %*{ "status": "active" }
    let docs = db.query()
      .where("type", "==", "test")
      .filter(filter)
      .list()
    check docs.len == 4

  test "Filter with numeric value (implicit $eq)":
    let filter = %*{ "priority": 5 }
    let docs = db.query()
      .where("type", "==", "test")
      .filter(filter)
      .list()
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-5"


suite "SimpleDB Count and Distinct":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    for i in 0 ..< 10:
      db.put(%*{
        "id": "doc-" & $i,
        "type": "count-test",
        "category": if i mod 3 == 0: "A" elif i mod 3 == 1: "B" else: "C"
      })

  teardown:
    db.close()

  test "Count documents":
    let count = db.query()
      .where("type", "==", "count-test")
      .count()
    check count == 10

  test "Count with filter":
    let filter = %*{ "category": "A" }
    let count = db.query()
      .where("type", "==", "count-test")
      .filter(filter)
      .count()
    check count == 4  # indices 0,3,6,9

  test "Distinct values":
    let values = db.query()
      .where("type", "==", "count-test")
      .distinctValues("category")
    check values.len == 3
    check "A" in values
    check "B" in values
    check "C" in values


suite "SimpleDB Update Operations":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    for i in 0 ..< 5:
      db.put(%*{
        "id": "doc-" & $i,
        "type": "update-test",
        "status": "pending",
        "count": i
      })

  teardown:
    db.close()

  test "Update single document":
    let updates = %*{ "status": "completed" }
    let numUpdated = db.query()
      .where("id", "==", "doc-2")
      .update(updates)
    check numUpdated == 1
    
    let doc = db.get("doc-2")
    check doc["status"].getStr == "completed"
    check doc["count"].getInt == 2  # Other fields preserved

  test "Update multiple documents":
    let updates = %*{ "status": "archived" }
    let numUpdated = db.query()
      .where("type", "==", "update-test")
      .update(updates)
    check numUpdated == 5
    
    for i in 0 ..< 5:
      let doc = db.get("doc-" & $i)
      check doc["status"].getStr == "archived"

  test "Update with $set":
    let updates = %*{ "$set": { "status": "active", "count": 100 } }
    let numUpdated = db.query()
      .where("id", "==", "doc-0")
      .limit(1)
      .update(updates)
    check numUpdated == 1
    
    let doc = db.get("doc-0")
    check doc["status"].getStr == "active"
    check doc["count"].getInt == 100

  test "Update multiple fields directly":
    let updates = %*{ "status": "archived", "count": 999 }
    let numUpdated = db.query()
      .where("id", "==", "doc-1")
      .limit(1)
      .update(updates)
    check numUpdated == 1
    
    let doc = db.get("doc-1")
    check doc["status"].getStr == "archived"
    check doc["count"].getInt == 999


suite "SimpleDB Remove Operations":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    for i in 0 ..< 10:
      db.put(%*{
        "id": "doc-" & $i,
        "type": "remove-test",
        "status": if i < 5: "old" else: "new"
      })

  teardown:
    db.close()

  test "Remove single document by query":
    let numRemoved = db.query()
      .where("id", "==", "doc-0")
      .limit(1)
      .remove()
    check numRemoved == 1
    check db.get("doc-0") == nil

  test "Remove multiple documents by query":
    let numRemoved = db.query()
      .where("status", "==", "old")
      .remove()
    check numRemoved == 5
    
    for i in 0 ..< 5:
      check db.get("doc-" & $i) == nil
    for i in 5 ..< 10:
      check db.get("doc-" & $i) != nil

  test "Remove with filter":
    let filter = %*{ "status": "new" }
    let numRemoved = db.query()
      .where("type", "==", "remove-test")
      .filter(filter)
      .remove()
    check numRemoved == 5


suite "SimpleDB Nested Field Queries (Dot Notation)":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    # Seed test data with nested objects
    db.put(%*{
      "id": "user-1",
      "name": "John Doe",
      "user": {"name": "John", "age": 30},
      "address": {"city": "NYC", "zipcode": "10001"},
      "tags": ["vip", "premium"]
    })
    db.put(%*{
      "id": "user-2",
      "name": "Jane Smith",
      "user": {"name": "Jane", "age": 25},
      "address": {"city": "LA", "zipcode": "90001"},
      "tags": ["standard"]
    })
    db.put(%*{
      "id": "user-3",
      "name": "Bob Wilson",
      "user": {"name": "Bob", "age": 35},
      "address": {"city": "NYC", "zipcode": "10002"},
      "tags": ["vip"]
    })

  teardown:
    db.close()

  test "Query nested field with dot notation (string)":
    let docs = db.query()
      .where("user.name", "==", "John")
      .list()
    check docs.len == 1
    check docs[0]["id"].getStr == "user-1"

  test "Query nested field with dot notation (number)":
    let docs = db.query()
      .where("user.age", ">=", 30)
      .list()
    check docs.len == 2

  test "Query deeply nested field":
    let docs = db.query()
      .where("address.city", "==", "NYC")
      .list()
    check docs.len == 2
    check docs[0]["id"].getStr in ["user-1", "user-3"]
    check docs[1]["id"].getStr in ["user-1", "user-3"]

  test "Query nested field with zipcode":
    let docs = db.query()
      .where("address.zipcode", "==", "90001")
      .list()
    check docs.len == 1
    check docs[0]["id"].getStr == "user-2"

  test "Query nested field with not equal operator":
    let docs = db.query()
      .where("address.city", "!=", "NYC")
      .list()
    check docs.len == 1
    check docs[0]["id"].getStr == "user-2"

  test "Query nested field with greater than operator":
    let docs = db.query()
      .where("user.age", ">", 25)
      .list()
    check docs.len == 2

  test "Query nested field with less than operator":
    let docs = db.query()
      .where("user.age", "<", 30)
      .list()
    check docs.len == 1
    check docs[0]["id"].getStr == "user-2"

  test "Query nested field combined with flat field":
    let docs = db.query()
      .where("address.city", "==", "NYC")
      .where("user.age", ">=", 30)
      .list()
    check docs.len == 2

  test "Query nested field with filter method":
    let filter = %*{ "user.name": "Jane" }
    let docs = db.query().filter(filter).list()
    check docs.len == 1
    check docs[0]["id"].getStr == "user-2"

  test "Query nested field with $eq operator in filter":
    let filter = %*{ "address.city": { "$eq": "LA" } }
    let docs = db.query().filter(filter).list()
    check docs.len == 1
    check docs[0]["id"].getStr == "user-2"


suite "SimpleDB Logical Operators ($or, $and)":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    # Seed test data
    db.put(%*{ "id": "doc-1", "type": "A", "status": "active", "priority": 1 })
    db.put(%*{ "id": "doc-2", "type": "A", "status": "inactive", "priority": 2 })
    db.put(%*{ "id": "doc-3", "type": "B", "status": "active", "priority": 3 })
    db.put(%*{ "id": "doc-4", "type": "B", "status": "inactive", "priority": 4 })
    db.put(%*{ "id": "doc-5", "type": "C", "status": "pending", "priority": 5 })

  teardown:
    db.close()

  test "Filter with $or operator":
    let filter = %*{
      "$or": [
        { "type": "A" },
        { "type": "B" }
      ]
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 4

  test "Filter with $or operator - multiple conditions":
    let filter = %*{
      "$or": [
        { "status": "active" },
        { "priority": { "$gte": 4 } }
      ]
    }
    let docs = db.query().filter(filter).list()
    # active: doc-1, doc-3 (2 docs)
    # priority >= 4: doc-4, doc-5 (2 docs)
    # Total unique: 4
    check docs.len == 4

  test "Filter with $and operator":
    let filter = %*{
      "$and": [
        { "type": "A" },
        { "status": "active" }
      ]
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-1"

  test "Filter with $and operator - multiple conditions":
    let filter = %*{
      "$and": [
        { "type": "B" },
        { "status": "inactive" },
        { "priority": { "$gte": 3 } }
      ]
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-4"

  test "Filter with $or combined with regular filter":
    let filter = %*{
      "status": "active",
      "$or": [
        { "type": "A" },
        { "type": "B" }
      ]
    }
    let docs = db.query().filter(filter).list()
    # status=active AND (type=A OR type=B)
    # doc-1 (A, active), doc-3 (B, active)
    check docs.len == 2

  test "Filter with $and combined with regular filter":
    let filter = %*{
      "type": "A",
      "$and": [
        { "status": "inactive" },
        { "priority": { "$gte": 1 } }
      ]
    }
    let docs = db.query().filter(filter).list()
    # type=A AND (status=inactive AND priority>=1)
    # doc-2 (A, inactive, priority=2)
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-2"


suite "SimpleDB Array Operators ($all, $size)":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    # Seed test data with arrays
    db.put(%*{
      "id": "doc-1",
      "name": "Item 1",
      "tags": ["a", "b", "c"],
      "categories": ["electronics"]
    })
    db.put(%*{
      "id": "doc-2",
      "name": "Item 2",
      "tags": ["a", "b"],
      "categories": ["electronics", "gadgets"]
    })
    db.put(%*{
      "id": "doc-3",
      "name": "Item 3",
      "tags": ["b", "c", "d"],
      "categories": ["books"]
    })
    db.put(%*{
      "id": "doc-4",
      "name": "Item 4",
      "tags": ["a"],
      "categories": []
    })

  teardown:
    db.close()

  test "Filter with $all operator - single value":
    let filter = %*{
      "tags": { "$all": ["a"] }
    }
    let docs = db.query().filter(filter).list()
    # doc-1 (a,b,c), doc-2 (a,b), doc-4 (a) contain "a"
    check docs.len == 3

  test "Filter with $all operator - multiple values":
    let filter = %*{
      "tags": { "$all": ["a", "b"] }
    }
    let docs = db.query().filter(filter).list()
    # doc-1 (a,b,c) and doc-2 (a,b) contain both "a" and "b"
    check docs.len == 2

  test "Filter with $all operator - all three values":
    let filter = %*{
      "tags": { "$all": ["a", "b", "c"] }
    }
    let docs = db.query().filter(filter).list()
    # Only doc-1 (a,b,c) contains all three
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-1"

  test "Filter with $size operator":
    let filter = %*{
      "tags": { "$size": 2 }
    }
    let docs = db.query().filter(filter).list()
    # doc-2 has 2 tags (a,b)
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-2"

  test "Filter with $size operator - empty array":
    let filter = %*{
      "categories": { "$size": 0 }
    }
    let docs = db.query().filter(filter).list()
    # doc-4 has empty categories
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-4"

  test "Filter with $size operator - three elements":
    let filter = %*{
      "tags": { "$size": 3 }
    }
    let docs = db.query().filter(filter).list()
    # doc-1 (a,b,c) and doc-3 (b,c,d) have 3 tags
    check docs.len == 2

  test "Filter combining $all with regular filter":
    let filter = %*{
      "name": "Item 1",
      "tags": { "$all": ["a", "b"] }
    }
    let docs = db.query().filter(filter).list()
    # name=Item1 AND tags contains (a,b)
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-1"

  test "Filter combining $size with $all":
    let filter = %*{
      "tags": { "$all": ["a"], "$size": 3 }
    }
    let docs = db.query().filter(filter).list()
    # tags contains "a" AND has 3 elements: doc-1
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-1"


suite "SimpleDB $exists Operator":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    # Seed test data - some with optional fields
    # Note: SQLite json_extract returns NULL for both non-existent fields AND null values
    # So $exists: true matches fields with non-null values
    db.put(%*{
      "id": "doc-1",
      "name": "Item 1",
      "status": "active",
      "archived": false
    })
    db.put(%*{
      "id": "doc-2",
      "name": "Item 2",
      "status": "inactive"
      # archived field does not exist
    })
    db.put(%*{
      "id": "doc-3",
      "name": "Item 3",
      "status": "active",
      "archived": true
    })
    db.put(%*{
      "id": "doc-4",
      "name": "Item 4"
      # status and archived do not exist
    })

  teardown:
    db.close()

  test "Filter with $exists: true - field has non-null value":
    let filter = %*{
      "status": { "$exists": true }
    }
    let docs = db.query().filter(filter).list()
    # doc-1, doc-2, doc-3 have status field with non-null values
    check docs.len == 3

  test "Filter with $exists: false - field missing or null":
    let filter = %*{
      "status": { "$exists": false }
    }
    let docs = db.query().filter(filter).list()
    # doc-4 does not have status field
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-4"

  test "Filter with $exists: true on optional field":
    let filter = %*{
      "archived": { "$exists": true }
    }
    let docs = db.query().filter(filter).list()
    # doc-1 and doc-3 have archived field with non-null values
    check docs.len == 2

  test "Filter with $exists: false on optional field":
    let filter = %*{
      "archived": { "$exists": false }
    }
    let docs = db.query().filter(filter).list()
    # doc-2 and doc-4 do not have archived field
    check docs.len == 2

  test "Filter combining $exists: true with value filter":
    let filter = %*{
      "status": "active",
      "archived": { "$exists": true }
    }
    let docs = db.query().filter(filter).list()
    # status=active AND archived exists with value
    # doc-1 and doc-3
    check docs.len == 2

  test "Filter combining $exists: false with other filters":
    let filter = %*{
      "status": "inactive",
      "archived": { "$exists": false }
    }
    let docs = db.query().filter(filter).list()
    # status=inactive AND archived does not exist
    # Only doc-2
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-2"
