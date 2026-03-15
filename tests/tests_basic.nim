import unittest, json, os
import njsdb

suite "NJSDB Basic Operations":
  var db: NJSDB

  setup:
    db = newNJSDB()
    db.open(":memory:")
    discard db.collection("test")

  teardown:
    db.close()

  test "Put and get document":
    let doc = %*{ "id": "doc1", "name": "Test Document", "value": 42 }
    db.put(doc)

    let retrieved = db.get("doc1")
    check retrieved != nil
    check retrieved["name"].getStr == "Test Document"
    check retrieved["value"].getInt == 42

  test "Put and get multiple documents":
    db.put(%*{ "id": "user1", "name": "Alice", "age": 30 })
    db.put(%*{ "id": "user2", "name": "Bob", "age": 25 })
    db.put(%*{ "id": "user3", "name": "Charlie", "age": 35 })

    let user1 = db.get("user1")
    let user2 = db.get("user2")
    let user3 = db.get("user3")

    check user1["name"].getStr == "Alice"
    check user2["name"].getStr == "Bob"
    check user3["name"].getStr == "Charlie"

  test "Update document with merge":
    let doc = %*{ "id": "doc1", "name": "Original", "status": "active" }
    db.put(doc)

    let update = %*{ "id": "doc1", "status": "inactive" }
    db.put(update, merge = true)

    let retrieved = db.get("doc1")
    check retrieved["name"].getStr == "Original"
    check retrieved["status"].getStr == "inactive"

  test "Remove document":
    db.put(%*{ "id": "doc1", "name": "To Delete" })
    check db.get("doc1") != nil

    discard db.delete("doc1")
    check db.get("doc1") == nil

  test "Query with where clause":
    db.put(%*{ "id": "user1", "name": "Alice", "age": 30 })
    db.put(%*{ "id": "user2", "name": "Bob", "age": 25 })
    db.put(%*{ "id": "user3", "name": "Charlie", "age": 35 })

    var q = db.query()
    let results = q.where("age", ">", 28).list()
    check results.len == 2

  test "Query with sort":
    db.put(%*{ "id": "user1", "name": "Alice", "score": 100 })
    db.put(%*{ "id": "user2", "name": "Bob", "score": 50 })
    db.put(%*{ "id": "user3", "name": "Charlie", "score": 75 })

    var q = db.query()
    let results = q.sort("score", ascending = true).list()
    check results.len == 3
    check results[0]["name"].getStr == "Bob"
    check results[1]["name"].getStr == "Charlie"
    check results[2]["name"].getStr == "Alice"

  test "Query with limit and offset":
    for i in 1..10:
      db.put(%*{ "id": "doc" & $i, "index": i })

    var q = db.query()
    let page1 = q.sort("index", ascending = true).limit(3).offset(0).list()
    check page1.len == 3
    check page1[0]["index"].getInt == 1

    var q2 = db.query()
    let page2 = q2.sort("index", ascending = true).limit(3).offset(3).list()
    check page2.len == 3
    check page2[0]["index"].getInt == 4

  test "Query with count":
    for i in 1..5:
      db.put(%*{ "id": "doc" & $i, "active": i <= 3 })

    var q = db.query()
    let total = q.count()
    check total == 5

    let activeFilter = %*{ "active": true }
    var q2 = db.query()
    let active = q2.filter(activeFilter).count()
    check active == 3

  test "Query with filter":
    db.put(%*{ "id": "user1", "name": "Alice", "role": "admin" })
    db.put(%*{ "id": "user2", "name": "Bob", "role": "user" })
    db.put(%*{ "id": "user3", "name": "Charlie", "role": "admin" })

    let filter = %*{
      "role": {
        "$in": ["admin"]
      }
    }

    var q = db.query()
    let admins = q.filter(filter).list()
    check admins.len == 2

  test "Query with $in operator":
    db.put(%*{ "id": "user1", "name": "Alice", "role": "admin" })
    db.put(%*{ "id": "user2", "name": "Bob", "role": "user" })
    db.put(%*{ "id": "user3", "name": "Charlie", "role": "moderator" })

    let filter = %*{
      "role": {
        "$in": ["admin", "moderator"]
      }
    }

    var q = db.query()
    let results = q.filter(filter).list()
    check results.len == 2

  test "Query with $nin operator":
    db.put(%*{ "id": "user1", "name": "Alice", "role": "admin" })
    db.put(%*{ "id": "user2", "name": "Bob", "role": "user" })
    db.put(%*{ "id": "user3", "name": "Charlie", "role": "moderator" })

    let filter = %*{
      "role": {
        "$nin": ["admin"]
      }
    }

    var q = db.query()
    let results = q.filter(filter).list()
    check results.len == 2

  test "Query with $not operator":
    db.put(%*{ "id": "user1", "name": "Alice", "active": true })
    db.put(%*{ "id": "user2", "name": "Bob", "active": false })
    db.put(%*{ "id": "user3", "name": "Charlie", "active": true })

    let filter = %*{
      "active": {
        "$not": true
      }
    }

    var q = db.query()
    let results = q.filter(filter).list()
    check results.len == 1
    check results[0]["name"].getStr == "Bob"

  test "Query with $nor operator":
    db.put(%*{ "id": "user1", "name": "Alice", "role": "admin", "active": true })
    db.put(%*{ "id": "user2", "name": "Bob", "role": "user", "active": false })
    db.put(%*{ "id": "user3", "name": "Charlie", "role": "user", "active": true })

    let filter = %*{
      "$nor": [
        { "role": "admin" },
        { "active": false }
      ]
    }

    var q = db.query()
    let results = q.filter(filter).list()
    check results.len == 1
    check results[0]["name"].getStr == "Charlie"

  test "Query with boolean filter":
    db.put(%*{ "id": "user1", "name": "Alice", "active": true })
    db.put(%*{ "id": "user2", "name": "Bob", "active": false })

    let filter = %*{
      "active": true
    }

    var q = db.query()
    let results = q.filter(filter).list()
    check results.len == 1
    check results[0]["name"].getStr == "Alice"

  test "Update with $set":
    db.put(%*{ "id": "user1", "name": "Alice", "age": 30, "status": "active" })

    var q = db.query()
    discard q.where("id", "==", "user1").update(%*{
      "$set": { "age": 31, "status": "inactive" }
    })

    let updated = db.get("user1")
    check updated["age"].getInt == 31
    check updated["status"].getStr == "inactive"
    check updated["name"].getStr == "Alice"

  test "Auto-generated ID":
    let doc = %*{ "name": "No ID Document", "value": 123 }
    db.upsert(doc)

    # Query to get the document back with its auto-generated ID
    var q = db.query()
    let results = q.where("name", "==", "No ID Document").list()
    check results.len == 1
    check results[0]["id"].getStr.len > 0
    check results[0]["name"].getStr == "No ID Document"

  test "Distinct values":
    db.put(%*{ "id": "user1", "department": "Engineering" })
    db.put(%*{ "id": "user2", "department": "Engineering" })
    db.put(%*{ "id": "user3", "department": "Sales" })
    db.put(%*{ "id": "user4", "department": "Sales" })
    db.put(%*{ "id": "user5", "department": "Marketing" })

    var q = db.query()
    let depts = q.distinctValues("department")
    check depts.len == 3

  test "Iterator":
    for i in 1..5:
      db.put(%*{ "id": "doc" & $i, "index": i })

    var count = 0
    var q = db.query()
    for doc in q.list():
      count += 1

    check count == 5


suite "NJSDB Collections":
  var db: NJSDB

  setup:
    db = newNJSDB()
    db.open(":memory:")

  teardown:
    db.close()

  test "Must call collection() before operations":
    expect NJSDBError:
      db.put(%*{ "id": "doc1", "name": "Test" })

  test "Switch between collections":
    db.collection("users").put(%*{ "id": "user1", "name": "Alice", "age": 30 })
    db.collection("users").put(%*{ "id": "user2", "name": "Bob", "age": 25 })

    db.collection("orders").put(%*{ "id": "order1", "userId": "user1", "total": 100 })
    db.collection("orders").put(%*{ "id": "order2", "userId": "user2", "total": 200 })

    var q1 = db.collection("users").query()
    check q1.count() == 2

    var q2 = db.collection("orders").query()
    check q2.count() == 2

    check db.collection("users").get("order1") == nil
    check db.collection("orders").get("user1") == nil

  test "Collection method chaining":
    let collectionResult = db.collection("products")
    check collectionResult is NJSDB

  test "Query across different collections":
    db.collection("active").put(%*{ "id": "doc1", "status": "active" })
    db.collection("archived").put(%*{ "id": "doc1", "status": "archived" })

    var q1 = db.collection("active").query()
    let active = q1.where("status", "==", "active").list()
    check active.len == 1
    check active[0]["status"].getStr == "active"

    var q2 = db.collection("archived").query()
    let archived = q2.where("status", "==", "archived").list()
    check archived.len == 1
    check archived[0]["status"].getStr == "archived"

  test "Update in specific collection":
    db.collection("inventory").put(%*{ "id": "item1", "name": "Widget", "stock": 100 })
    db.collection("archive").put(%*{ "id": "item1", "name": "Old Widget", "stock": 0 })

    var q = db.collection("inventory").query()
    discard q.where("id", "==", "item1").update(%*{
      "$inc": { "stock": -5 }
    })

    let item = db.collection("inventory").get("item1")
    check item["stock"].getInt == 95

    let archived = db.collection("archive").get("item1")
    check archived["stock"].getInt == 0

  test "Delete from specific collection":
    db.collection("active").put(%*{ "id": "doc1", "status": "active" })
    db.collection("archived").put(%*{ "id": "doc1", "status": "archived" })

    let deleted = db.collection("active").delete("doc1")
    check deleted == true

    check db.collection("active").get("doc1") == nil
    check db.collection("archived").get("doc1") != nil


# Clean up test database
removeFile("test.db")
