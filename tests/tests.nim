import std/[unittest, json, os]
import simpledb

suite "SimpleDB":

    test "Create in-memory database":

        # Create it
        var db = SimpleDB.init(":memory:")

        # Check we can put and get
        db.put(%*{"id": "test", "value": 123})
        let doc = db.get("test")
        check doc["value"].getInt == 123

        # Clean up
        db.close()

    test "Put and get document":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create a document
        var doc = %*{ "id": "doc1", "name": "Test Document", "value": 42 }

        # Put it
        db.put(doc)

        # Get it back
        var retrieved = db.get("doc1")

        # Check it
        check retrieved["name"].getStr == "Test Document"
        check retrieved["value"].getInt == 42

        # Clean up
        db.close()

    test "Put and get multiple documents":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create some documents
        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "name": "Document " & $i,
                "number": i
            }
            db.put(doc)

        # Get them back
        for i in 1..10:
            var retrieved = db.get("doc" & $i)
            check retrieved["name"].getStr == "Document " & $i
            check retrieved["number"].getInt == i

        # Clean up
        db.close()

    test "Update document":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create a document
        var doc = %*{ "id": "doc1", "name": "Original Name", "value": 42 }
        db.put(doc)

        # Update it
        var updated = %*{ "id": "doc1", "name": "Updated Name" }
        db.put(updated, merge = true)

        # Get it back
        var retrieved = db.get("doc1")

        # Check it - should have merged
        check retrieved["name"].getStr == "Updated Name"
        check retrieved["value"].getInt == 42

        # Clean up
        db.close()

    test "Remove document":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create a document
        var doc = %*{ "id": "doc1", "name": "Test Document" }
        db.put(doc)

        # Remove it
        let removed = db.removeOne("doc1")
        check removed == true

        # Try to get it back - should return nil
        var retrieved = db.get("doc1")
        check retrieved == nil

        # Try to remove non-existent document
        let removedAgain = db.removeOne("doc1")
        check removedAgain == false

        # Clean up
        db.close()

    test "Query with where":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create some documents
        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "name": "Document " & $i,
                "number": i,
                "category": if i mod 2 == 0: "even" else: "odd"
            }
            db.put(doc)

        # Query for even numbers
        var results = db.query().where("category", "==", "even").list()
        check results.len == 5

        # Query for numbers greater than 5
        results = db.query().where("number", ">", 5).list()
        check results.len == 5

        # Clean up
        db.close()

    test "Query with sort":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create some documents
        for i in 1..5:
            var doc = %*{
                "id": "doc" & $i,
                "value": 6 - i  # 5, 4, 3, 2, 1
            }
            db.put(doc)

        # Query with ascending sort
        var results = db.query().sort("value", true).list()
        check results[0]["value"].getFloat == 1.0
        check results[4]["value"].getFloat == 5.0

        # Query with descending sort
        results = db.query().sort("value", false).list()
        check results[0]["value"].getFloat == 5.0
        check results[4]["value"].getFloat == 1.0

        # Clean up
        db.close()

    test "Query with limit and offset":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create some documents
        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "number": i
            }
            db.put(doc)

        # Query with limit
        var results = db.query().sort("number", true).limit(3).list()
        check results.len == 3
        check results[0]["number"].getInt == 1
        check results[2]["number"].getInt == 3

        # Query with offset
        results = db.query().sort("number", true).offset(5).list()
        check results.len == 5
        check results[0]["number"].getInt == 6

        # Query with limit and offset
        results = db.query().sort("number", true).offset(3).limit(3).list()
        check results.len == 3
        check results[0]["number"].getInt == 4
        check results[2]["number"].getInt == 6

        # Clean up
        db.close()

    test "Query with count":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create some documents
        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "category": if i <= 5: "A" else: "B"
            }
            db.put(doc)

        # Count all
        check db.query().count() == 10

        # Count category A
        check db.query().where("category", "==", "A").count() == 5

        # Count category B
        check db.query().where("category", "==", "B").count() == 5

        # Clean up
        db.close()

    test "Query with filter":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create some documents
        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "status": if i <= 5: "active" else: "inactive",
                "priority": i
            }
            db.put(doc)

        # Query with filter
        var filter = %*{
            "status": "active"
        }
        var results = db.query().filter(filter).list()
        check results.len == 5

        # Query with multiple conditions
        filter = %*{
            "status": "active",
            "priority": 3
        }
        results = db.query().filter(filter).list()
        check results.len == 1
        check results[0]["id"].getStr == "doc3"

        # Clean up
        db.close()

    test "Query with $in operator":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create some documents
        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "type": "type" & $(i mod 3)
            }
            db.put(doc)

        # Query with $in
        var filter = %*{
            "type": { "$in": ["type0", "type1"] }
        }
        var results = db.query().filter(filter).list()
        check results.len == 7  # type0: 4 docs, type1: 3 docs

        # Clean up
        db.close()

    test "Query with boolean filter":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create some documents with boolean field
        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "name": "Document " & $i,
                "active": i <= 5  # First 5 are active=true
            }
            db.put(doc)

        # Query with boolean true
        var filter = %*{
            "active": true
        }
        var results = db.query().filter(filter).list()
        check results.len == 5

        # Query with boolean false
        filter = %*{
            "active": false
        }
        results = db.query().filter(filter).list()
        check results.len == 5

        # Clean up
        db.close()

    test "Update with $set":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create a document
        var doc = %*{
            "id": "doc1",
            "name": "Original",
            "value": 100,
            "tags": ["a", "b"]
        }
        db.put(doc)

        # Update with $set
        db.query().where("id", "==", "doc1").update(%*{
            "$set": {
                "name": "Updated",
                "value": 200
            }
        })

        # Get it back
        var retrieved = db.get("doc1")
        check retrieved["name"].getStr == "Updated"
        check retrieved["value"].getInt == 200
        check retrieved["tags"].len == 2  # Should preserve existing fields

        # Clean up
        db.close()

    test "Update with $set using complex array":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create a document
        var doc = %*{
            "id": "doc1",
            "name": "Original",
            "options": []
        }
        db.put(doc)

        # Update with complex array using $set
        let matchedOptions = %*[
            {"id": "opt1", "label": "Option 1", "selected": true},
            {"id": "opt2", "label": "Option 2", "selected": false}
        ]
        
        db.query().where("id", "==", "doc1").update(%*{
            "$set": {
                "options": matchedOptions,
                "grade": true
            }
        })

        # Get it back
        var retrieved = db.get("doc1")
        check retrieved["options"].len == 2
        check retrieved["options"][0]["id"].getStr == "opt1"
        check retrieved["options"][0]["selected"].getBool == true
        check retrieved["grade"].getBool == true

        # Clean up
        db.close()

    test "Batch operations":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Insert multiple documents
        for i in 1..5:
            var doc = %*{
                "id": "batch" & $i,
                "value": i
            }
            db.put(doc)

        # Check all were inserted
        check db.query().count() == 5

        # Clean up
        db.close()

    test "Auto-generated ID":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create document without ID
        var doc = %*{
            "name": "No ID Document",
            "value": 42
        }

        # Put it - should generate ID
        db.put(doc)

        # Verify document was stored by querying for it
        check db.query().count() == 1
        var retrieved = db.query().where("name", "==", "No ID Document").get()
        check retrieved != nil
        check retrieved["name"].getStr == "No ID Document"
        check retrieved["value"].getInt == 42
        # Verify ID was generated
        check retrieved["id"].getStr.len > 0

        # Clean up
        db.close()

    test "Distinct values":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create documents with categories
        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "category": "cat" & $(i mod 3)
            }
            db.put(doc)

        # Get distinct categories
        var distinctValues = db.query().distinctValues("category")
        check distinctValues.len == 3

        # Clean up
        db.close()

    test "Query with filter and count":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create documents with categories
        for i in 1..10:
            var doc = %*{
                "id": "doc" & $i,
                "category": if i <= 7: "A" else: "B"
            }
            db.put(doc)

        # Count by category using filter
        check db.query().where("category", "==", "A").count() == 7
        check db.query().where("category", "==", "B").count() == 3

        # Clean up
        db.close()

    test "Iterator":

        # Create database
        var db = SimpleDB.init(":memory:")

        # Create some documents
        for i in 1..5:
            var doc = %*{
                "id": "doc" & $i,
                "number": i
            }
            db.put(doc)

        # Iterate through documents
        var count = 0
        for doc in db.query().list():
            count += 1

        check count == 5

        # Clean up
        db.close()


# Clean up test database
removeFile("test.db")


suite "SimpleDB Nested Field Queries":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    # Seed test data with nested objects
    db.put(%*{
      "id": "user-1",
      "name": "John Doe",
      "address": {
        "street": "123 Main St",
        "city": "New York",
        "zipcode": "10001"
      },
      "profile": {
        "age": 30,
        "settings": {
          "theme": "dark"
        }
      }
    })
    db.put(%*{
      "id": "user-2",
      "name": "Jane Smith",
      "address": {
        "street": "456 Oak Ave",
        "city": "Los Angeles",
        "zipcode": "90001"
      },
      "profile": {
        "age": 25,
        "settings": {
          "theme": "light"
        }
      }
    })
    db.put(%*{
      "id": "user-3",
      "name": "Bob Wilson",
      "address": {
        "street": "789 Pine Rd",
        "city": "New York",
        "zipcode": "10002"
      },
      "profile": {
        "age": 35,
        "settings": {
          "theme": "dark"
        }
      }
    })

  teardown:
    db.close()

  test "Query nested field with where method":
    let docs = db.query().where("address.city", "==", "New York").list()
    check docs.len == 2
    check docs[0]["id"].getStr == "user-1"
    check docs[1]["id"].getStr == "user-3"

  test "Query deeply nested field":
    let docs = db.query().where("profile.settings.theme", "==", "dark").list()
    check docs.len == 2

  test "Query nested field with comparison operator":
    let docs = db.query().where("profile.age", ">", 28).list()
    check docs.len == 2
    check docs[0]["id"].getStr == "user-1"
    check docs[1]["id"].getStr == "user-3"

  test "Query nested field combined with flat field":
    let docs = db.query().where("address.city", "==", "New York").where("profile.age", ">", 30).list()
    check docs.len == 1
    check docs[0]["id"].getStr == "user-3"

  test "Query nested field with filter method":
    let filter = %*{
      "address.zipcode": "90001"
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1
    check docs[0]["id"].getStr == "user-2"

  test "Query nested field with $eq operator in filter":
    let filter = %*{
      "profile.settings.theme": { "$eq": "light" }
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1
    check docs[0]["id"].getStr == "user-2"


suite "SimpleDB Logical Operators ($or, $and)":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    # Seed test data
    db.put(%*{
      "id": "doc-1",
      "type": "A",
      "status": "active",
      "priority": 5
    })
    db.put(%*{
      "id": "doc-2",
      "type": "A",
      "status": "inactive",
      "priority": 3
    })
    db.put(%*{
      "id": "doc-3",
      "type": "B",
      "status": "active",
      "priority": 8
    })
    db.put(%*{
      "id": "doc-4",
      "type": "B",
      "status": "inactive",
      "priority": 2
    })

  teardown:
    db.close()

  test "Filter with $or operator":
    let filter = %*{
      "$or": [
        { "type": "A" },
        { "status": "active" }
      ]
    }
    let docs = db.query().filter(filter).list()
    # doc-1 (A, active), doc-2 (A, inactive), doc-3 (B, active)
    check docs.len == 3

  test "Filter with $or operator - multiple conditions":
    let filter = %*{
      "$or": [
        { "priority": { "$lt": 3 } },
        { "priority": { "$gt": 7 } }
      ]
    }
    let docs = db.query().filter(filter).list()
    # doc-4 (priority 2), doc-3 (priority 8)
    check docs.len == 2

  test "Filter with $and operator":
    let filter = %*{
      "$and": [
        { "type": "A" },
        { "status": "active" }
      ]
    }
    let docs = db.query().filter(filter).list()
    # Only doc-1
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-1"

  test "Filter with $and operator - multiple conditions":
    let filter = %*{
      "$and": [
        { "type": "B" },
        { "status": "active" },
        { "priority": { "$gte": 5 } }
      ]
    }
    let docs = db.query().filter(filter).list()
    # Only doc-3
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-3"

  test "Filter with $or combined with regular filter":
    let filter = %*{
      "status": "active",
      "$or": [
        { "type": "A" },
        { "priority": { "$gt": 6 } }
      ]
    }
    let docs = db.query().filter(filter).list()
    # status=active AND (type=A OR priority>6)
    # doc-1 (active, A), doc-3 (active, priority 8)
    check docs.len == 2

  test "Filter with $and combined with regular filter":
    let filter = %*{
      "type": "A",
      "$and": [
        { "status": "inactive" },
        { "priority": { "$lt": 5 } }
      ]
    }
    let docs = db.query().filter(filter).list()
    # type=A AND status=inactive AND priority<5
    # Only doc-2
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
      "tags": ["important", "urgent", "review"]
    })
    db.put(%*{
      "id": "doc-2",
      "name": "Item 2",
      "tags": ["important", "review"]
    })
    db.put(%*{
      "id": "doc-3",
      "name": "Item 3",
      "tags": ["urgent"]
    })
    db.put(%*{
      "id": "doc-4",
      "name": "Item 4",
      "tags": []  # Empty array
    })

  teardown:
    db.close()

  test "Filter with $all operator - single value":
    let filter = %*{
      "tags": { "$all": ["important"] }
    }
    let docs = db.query().filter(filter).list()
    # doc-1 and doc-2 have "important"
    check docs.len == 2

  test "Filter with $all operator - multiple values":
    let filter = %*{
      "tags": { "$all": ["important", "review"] }
    }
    let docs = db.query().filter(filter).list()
    # Only doc-1 and doc-2 have both "important" and "review"
    check docs.len == 2

  test "Filter with $all operator - all three values":
    let filter = %*{
      "tags": { "$all": ["important", "urgent", "review"] }
    }
    let docs = db.query().filter(filter).list()
    # Only doc-1 has all three
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-1"

  test "Filter with $size operator":
    let filter = %*{
      "tags": { "$size": 2 }
    }
    let docs = db.query().filter(filter).list()
    # doc-2 has 2 tags
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-2"

  test "Filter with $size operator - empty array":
    let filter = %*{
      "tags": { "$size": 0 }
    }
    let docs = db.query().filter(filter).list()
    # doc-4 has empty tags array
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-4"

  test "Filter with $size operator - three elements":
    let filter = %*{
      "tags": { "$size": 3 }
    }
    let docs = db.query().filter(filter).list()
    # doc-1 has 3 tags
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-1"

  test "Filter combining $all with regular filter":
    let filter = %*{
      "name": "Item 1",
      "tags": { "$all": ["urgent"] }
    }
    let docs = db.query().filter(filter).list()
    # Only doc-1 matches both conditions
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-1"

  test "Filter combining $size with $all":
    let filter = %*{
      "tags": {
        "$size": 2,
        "$all": ["important"]
      }
    }
    let docs = db.query().filter(filter).list()
    # doc-2 has 2 tags and contains "important"
    check docs.len == 1
    check docs[0]["id"].getStr == "doc-2"


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


suite "SimpleDB Projection":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    # Seed test data
    db.put(%*{
      "id": "doc-1",
      "name": "John Doe",
      "email": "john@example.com",
      "password": "secret123",
      "age": 30,
      "address": {
        "city": "New York",
        "zipcode": "10001"
      }
    })
    db.put(%*{
      "id": "doc-2",
      "name": "Jane Smith",
      "email": "jane@example.com",
      "password": "secret456",
      "age": 25,
      "address": {
        "city": "Los Angeles",
        "zipcode": "90001"
      }
    })

  teardown:
    db.close()

  test "Project with include - single field":
    let projection = %*{
      "name": 1
    }
    let docs = db.query().project(projection).list()
    check docs.len == 2
    check docs[0].hasKey("name")
    check docs[0]["name"].getStr == "John Doe"
    check not docs[0].hasKey("email")
    check not docs[0].hasKey("password")

  test "Project with include - multiple fields":
    let projection = %*{
      "name": 1,
      "email": 1
    }
    let docs = db.query().project(projection).list()
    check docs.len == 2
    check docs[0].hasKey("name")
    check docs[0].hasKey("email")
    check not docs[0].hasKey("password")
    check not docs[0].hasKey("age")

  test "Project with exclude - single field":
    let projection = %*{
      "password": 0
    }
    let docs = db.query().project(projection).list()
    check docs.len == 2
    check docs[0].hasKey("name")
    check docs[0].hasKey("email")
    check not docs[0].hasKey("password")

  test "Project with exclude - multiple fields":
    let projection = %*{
      "password": 0,
      "age": 0
    }
    let docs = db.query().project(projection).list()
    check docs.len == 2
    check docs[0].hasKey("name")
    check docs[0].hasKey("email")
    check not docs[0].hasKey("password")
    check not docs[0].hasKey("age")

  test "Project with include combined with filter":
    let filter = %*{
      "age": { "$gte": 25 }
    }
    let projection = %*{
      "name": 1,
      "email": 1
    }
    let docs = db.query().filter(filter).project(projection).list()
    check docs.len == 2
    check docs[0].hasKey("name")
    check docs[0].hasKey("email")
    check not docs[0].hasKey("password")

  test "Project with nested field include":
    let projection = %*{
      "name": 1,
      "address.city": 1
    }
    let docs = db.query().project(projection).list()
    check docs.len == 2
    check docs[0].hasKey("name")
    check docs[0].hasKey("address")
    check docs[0]["address"].hasKey("city")
    check not docs[0].hasKey("email")


suite "SimpleDB Extended Aggregation":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")
    # Seed test data - sales data
    db.put(%*{ "id": "sale-1", "category": "electronics", "amount": 100.0, "quantity": 2 })
    db.put(%*{ "id": "sale-2", "category": "electronics", "amount": 200.0, "quantity": 1 })
    db.put(%*{ "id": "sale-3", "category": "electronics", "amount": 150.0, "quantity": 3 })
    db.put(%*{ "id": "sale-4", "category": "clothing", "amount": 50.0, "quantity": 5 })
    db.put(%*{ "id": "sale-5", "category": "clothing", "amount": 75.0, "quantity": 2 })

  teardown:
    db.close()

  test "Aggregate with sum":
    let result = db.aggregate("category", %*{ "$sum": "amount" })
    check result.len == 2
    
    # Find electronics category
    var electronicsFound = false
    var clothingFound = false
    for agg in result:
      if agg.groupId == "electronics":
        check agg.sum == 450.0  # 100 + 200 + 150
        electronicsFound = true
      elif agg.groupId == "clothing":
        check agg.sum == 125.0  # 50 + 75
        clothingFound = true
    check electronicsFound
    check clothingFound

  test "Aggregate with avg":
    let result = db.aggregate("category", %*{ "$avg": "amount" })
    check result.len == 2
    
    for agg in result:
      if agg.groupId == "electronics":
        check agg.avg == 150.0  # (100 + 200 + 150) / 3
      elif agg.groupId == "clothing":
        check agg.avg == 62.5   # (50 + 75) / 2

  test "Aggregate with min and max":
    let result = db.aggregate("category", %*{ "$min": "amount", "$max": "amount" })
    check result.len == 2
    
    for agg in result:
      if agg.groupId == "electronics":
        check agg.min == 100.0
        check agg.max == 200.0
      elif agg.groupId == "clothing":
        check agg.min == 50.0
        check agg.max == 75.0

  test "Aggregate with multiple operators":
    let result = db.aggregate("category", %*{ "$sum": "amount", "$avg": "quantity" })
    check result.len == 2
    
    for agg in result:
      if agg.groupId == "electronics":
        check agg.sum == 450.0
        check agg.avg == 2.0  # (2 + 1 + 3) / 3
        check agg.count == 3
      elif agg.groupId == "clothing":
        check agg.sum == 125.0
        check agg.avg == 3.5  # (5 + 2) / 2
        check agg.count == 2

  test "Aggregate with filter":
    let filter = %*{ "amount": { "$gte": 100 } }
    let result = db.aggregate("category", %*{ "$sum": "amount" }, filter)
    
    # Only electronics should remain (all have amount >= 100)
    check result.len == 1
    check result[0].groupId == "electronics"
    check result[0].sum == 450.0


suite "SimpleDB Bulk Operations":
  var db: SimpleDB

  setup:
    db = SimpleDB.init(":memory:")

  teardown:
    db.close()

  test "Bulk insert documents":
    let docs = @[
      %*{ "id": "bulk-1", "name": "Item 1", "value": 10 },
      %*{ "id": "bulk-2", "name": "Item 2", "value": 20 },
      %*{ "id": "bulk-3", "name": "Item 3", "value": 30 },
      %*{ "id": "bulk-4", "name": "Item 4", "value": 40 },
      %*{ "id": "bulk-5", "name": "Item 5", "value": 50 }
    ]
    
    let inserted = db.bulkInsert(docs)
    check inserted == 5
    check db.query().count() == 5
    
    # Verify all documents were inserted correctly
    for i in 1..5:
      let doc = db.get("bulk-" & $i)
      check doc != nil
      check doc["name"].getStr == "Item " & $i
      check doc["value"].getInt == i * 10

  test "Bulk delete documents":
    # First insert some documents
    for i in 1..10:
      db.put(%*{ "id": "del-" & $i, "name": "Item " & $i })
    
    check db.query().count() == 10
    
    # Bulk delete specific IDs
    let idsToDelete = @["del-2", "del-4", "del-6", "del-8", "del-10"]
    let deleted = db.bulkDelete(idsToDelete)
    check deleted == 5
    check db.query().count() == 5
    
    # Verify correct documents were deleted
    for i in 1..10:
      let doc = db.get("del-" & $i)
      if i mod 2 == 0:  # Even numbers were deleted
        check doc == nil
      else:
        check doc != nil

  test "Bulk insert with auto-generated IDs":
    let docs = @[
      %*{ "name": "Auto 1", "value": 1 },
      %*{ "name": "Auto 2", "value": 2 },
      %*{ "name": "Auto 3", "value": 3 }
    ]
    
    let inserted = db.bulkInsert(docs)
    check inserted == 3
    
    # Check that documents were inserted with generated IDs
    check db.query().count() == 3
    for i in 1..3:
      let retrieved = db.query().where("name", "==", "Auto " & $i).get()
      check retrieved != nil
      check retrieved["id"].getStr.len > 0

  test "Bulk operations with empty input":
    let emptyDocs: seq[JsonNode] = @[]
    let inserted = db.bulkInsert(emptyDocs)
    check inserted == 0
    
    let emptyIds: seq[string] = @[]
    let deleted = db.bulkDelete(emptyIds)
    check deleted == 0
