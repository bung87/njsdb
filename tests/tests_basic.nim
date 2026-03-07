import std/[unittest, json, os]
import njsdb

suite "NJSDB Basic Operations":

    test "Create in-memory database":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        db.put(%*{"id": "test", "value": 123})
        let doc = db.get("test")
        check doc["value"].getInt == 123

        db.close()

    test "Put and get document":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        var doc = %*{ "id": "doc1", "name": "Test Document", "value": 42 }
        db.put(doc)

        var retrieved = db.get("doc1")
        check retrieved["name"].getStr == "Test Document"
        check retrieved["value"].getInt == 42

        db.close()

    test "Put and get multiple documents":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "name": "Document " & $i,
                "number": i
            }
            db.put(doc)

        for i in 1..10:
            var retrieved = db.get("doc" & $i)
            check retrieved["name"].getStr == "Document " & $i
            check retrieved["number"].getInt == i

        db.close()

    test "Update document with merge":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        var doc = %*{ "id": "doc1", "name": "Original Name", "value": 42 }
        db.put(doc)

        var updated = %*{ "id": "doc1", "name": "Updated Name" }
        db.put(updated, merge = true)

        var retrieved = db.get("doc1")
        check retrieved["name"].getStr == "Updated Name"
        check retrieved["value"].getInt == 42

        db.close()

    test "Remove document":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        var doc = %*{ "id": "doc1", "name": "Test Document" }
        db.put(doc)

        let removed = db.removeOne("doc1")
        check removed == true

        var retrieved = db.get("doc1")
        check retrieved == nil

        let removedAgain = db.removeOne("doc1")
        check removedAgain == false

        db.close()

    test "Query with where clause":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "name": "Document " & $i,
                "number": i,
                "category": if i mod 2 == 0: "even" else: "odd"
            }
            db.put(doc)

        var results = db.query().where("category", "==", "even").list()
        check results.len == 5

        results = db.query().where("number", ">", 5).list()
        check results.len == 5

        db.close()

    test "Query with sort":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        for i in 1..5:
            var doc = %*{
                "id": "doc" & $i,
                "value": 6 - i
            }
            db.put(doc)

        var results = db.query().sort("value", true).list()
        check results[0]["value"].getFloat == 1.0
        check results[4]["value"].getFloat == 5.0

        results = db.query().sort("value", false).list()
        check results[0]["value"].getFloat == 5.0
        check results[4]["value"].getFloat == 1.0

        db.close()

    test "Query with limit and offset":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "number": i
            }
            db.put(doc)

        var results = db.query().sort("number", true).limit(3).list()
        check results.len == 3
        check results[0]["number"].getInt == 1
        check results[2]["number"].getInt == 3

        results = db.query().sort("number", true).offset(5).list()
        check results.len == 5
        check results[0]["number"].getInt == 6

        results = db.query().sort("number", true).offset(3).limit(3).list()
        check results.len == 3
        check results[0]["number"].getInt == 4
        check results[2]["number"].getInt == 6

        db.close()

    test "Query with count":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "category": if i <= 5: "A" else: "B"
            }
            db.put(doc)

        check db.query().count() == 10
        check db.query().where("category", "==", "A").count() == 5
        check db.query().where("category", "==", "B").count() == 5

        db.close()

    test "Query with filter":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "status": if i <= 5: "active" else: "inactive",
                "priority": i
            }
            db.put(doc)

        var filter = %*{"status": "active"}
        var results = db.query().filter(filter).list()
        check results.len == 5

        filter = %*{"status": "active", "priority": 3}
        results = db.query().filter(filter).list()
        check results.len == 1
        check results[0]["id"].getStr == "doc3"

        db.close()

    test "Query with $in operator":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "type": "type" & $(i mod 3)
            }
            db.put(doc)

        var filter = %*{
            "type": { "$in": ["type0", "type1"] }
        }
        var results = db.query().filter(filter).list()
        check results.len == 7

        db.close()

    test "Query with boolean filter":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "name": "Document " & $i,
                "active": i <= 5
            }
            db.put(doc)

        var filter = %*{"active": true}
        var results = db.query().filter(filter).list()
        check results.len == 5

        filter = %*{"active": false}
        results = db.query().filter(filter).list()
        check results.len == 5

        db.close()

    test "Update with $set":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        var doc = %*{
            "id": "doc1",
            "name": "Original",
            "value": 100,
            "tags": ["a", "b"]
        }
        db.put(doc)

        db.query().where("id", "==", "doc1").update(%*{
            "$set": {
                "name": "Updated",
                "value": 200
            }
        })

        var retrieved = db.get("doc1")
        check retrieved["name"].getStr == "Updated"
        check retrieved["value"].getInt == 200
        check retrieved["tags"].len == 2

        db.close()

    test "Auto-generated ID":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        var doc = %*{
            "name": "No ID Document",
            "value": 42
        }

        db.put(doc)

        check db.query().count() == 1
        var retrieved = db.query().where("name", "==", "No ID Document").get()
        check retrieved != nil
        check retrieved["name"].getStr == "No ID Document"
        check retrieved["value"].getInt == 42
        check retrieved["id"].getStr.len > 0

        db.close()

    test "Distinct values":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "category": "cat" & $(i mod 3)
            }
            db.put(doc)

        var distinctValues = db.query().distinctValues("category")
        check distinctValues.len == 3

        db.close()

    test "Iterator":
        var db = NJSDB()
        db.open(":memory:")
        discard db.collection("documents")

        for i in 1..5:
            var doc = %*{
                "id": "doc" & $i,
                "number": i
            }
            db.put(doc)

        var count = 0
        for doc in db.query().list():
            count += 1

        check count == 5

        db.close()


suite "NJSDB Collections":
  var db: NJSDB

  setup:
    db = NJSDB()
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

    check db.collection("users").query().count() == 2
    check db.collection("orders").query().count() == 2

    check db.collection("users").get("order1") == nil
    check db.collection("orders").get("user1") == nil

  test "Collection method chaining":
    let result = db.collection("products")
    check result is NJSDB

  test "Query across different collections":
    db.collection("active").put(%*{ "id": "doc1", "status": "active" })
    db.collection("archived").put(%*{ "id": "doc1", "status": "archived" })

    let active = db.collection("active").query().where("status", "==", "active").list()
    check active.len == 1
    check active[0]["status"].getStr == "active"

    let archived = db.collection("archived").query().where("status", "==", "archived").list()
    check archived.len == 1
    check archived[0]["status"].getStr == "archived"

  test "Update in specific collection":
    db.collection("inventory").put(%*{ "id": "item1", "name": "Widget", "stock": 100 })
    db.collection("archive").put(%*{ "id": "item1", "name": "Old Widget", "stock": 0 })

    db.collection("inventory").query().where("id", "==", "item1").update(%*{
      "$inc": { "stock": -5 }
    })

    let item = db.collection("inventory").get("item1")
    check item["stock"].getInt == 95

    let archived = db.collection("archive").get("item1")
    check archived["stock"].getInt == 0

  test "Delete from specific collection":
    db.collection("active").put(%*{ "id": "doc1", "status": "active" })
    db.collection("archived").put(%*{ "id": "doc1", "status": "archived" })

    let deleted = db.collection("active").removeOne("doc1")
    check deleted == true

    check db.collection("active").get("doc1") == nil
    check db.collection("archived").get("doc1") != nil


# Clean up test database
removeFile("test.db")
