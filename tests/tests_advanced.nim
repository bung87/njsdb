import std/[unittest, json, os]
import simpledb

suite "SimpleDB Nested Field Queries":
  var db: SimpleDB

  setup:
    db = SimpleDB()
    db.open(":memory:")
    discard db.collection("documents")

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

  test "Query deeply nested field":
    let docs = db.query().where("profile.settings.theme", "==", "dark").list()
    check docs.len == 2

  test "Query nested field with comparison operator":
    let docs = db.query().where("profile.age", ">", 28).list()
    check docs.len == 2

  test "Query nested field combined with flat field":
    let docs = db.query().where("address.city", "==", "New York").where("profile.age", ">", 30).list()
    check docs.len == 1

  test "Query nested field with filter method":
    let filter = %*{
      "address.zipcode": "90001"
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1

  test "Query nested field with $eq operator in filter":
    let filter = %*{
      "profile.settings.theme": { "$eq": "light" }
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1


suite "SimpleDB Logical Operators ($or, $and)":
  var db: SimpleDB

  setup:
    db = SimpleDB()
    db.open(":memory:")
    discard db.collection("documents")

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
    check docs.len == 3

  test "Filter with $or operator - multiple conditions":
    let filter = %*{
      "$or": [
        { "priority": { "$lt": 3 } },
        { "priority": { "$gt": 7 } }
      ]
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 2

  test "Filter with $and operator":
    let filter = %*{
      "$and": [
        { "type": "A" },
        { "status": "active" }
      ]
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1

  test "Filter with $and operator - multiple conditions":
    let filter = %*{
      "$and": [
        { "type": "B" },
        { "status": "active" },
        { "priority": { "$gte": 5 } }
      ]
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1

  test "Filter with $or combined with regular filter":
    let filter = %*{
      "status": "active",
      "$or": [
        { "type": "A" },
        { "priority": { "$gt": 6 } }
      ]
    }
    let docs = db.query().filter(filter).list()
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
    check docs.len == 1


suite "SimpleDB Array Operators ($all, $size)":
  var db: SimpleDB

  setup:
    db = SimpleDB()
    db.open(":memory:")
    discard db.collection("documents")

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
      "tags": []
    })

  teardown:
    db.close()

  test "Filter with $all operator - single value":
    let filter = %*{
      "tags": { "$all": ["important"] }
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 2

  test "Filter with $all operator - multiple values":
    let filter = %*{
      "tags": { "$all": ["important", "review"] }
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 2

  test "Filter with $all operator - all three values":
    let filter = %*{
      "tags": { "$all": ["important", "urgent", "review"] }
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1

  test "Filter with $size operator":
    let filter = %*{
      "tags": { "$size": 2 }
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1

  test "Filter with $size operator - empty array":
    let filter = %*{
      "tags": { "$size": 0 }
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1

  test "Filter with $size operator - three elements":
    let filter = %*{
      "tags": { "$size": 3 }
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1


suite "SimpleDB $exists Operator":
  var db: SimpleDB

  setup:
    db = SimpleDB()
    db.open(":memory:")
    discard db.collection("documents")

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
    })

  teardown:
    db.close()

  test "Filter with $exists: true - field has non-null value":
    let filter = %*{
      "status": { "$exists": true }
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 3

  test "Filter with $exists: false - field missing or null":
    let filter = %*{
      "status": { "$exists": false }
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 1

  test "Filter with $exists: true on optional field":
    let filter = %*{
      "archived": { "$exists": true }
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 2

  test "Filter with $exists: false on optional field":
    let filter = %*{
      "archived": { "$exists": false }
    }
    let docs = db.query().filter(filter).list()
    check docs.len == 2


suite "SimpleDB Projection":
  var db: SimpleDB

  setup:
    db = SimpleDB()
    db.open(":memory:")
    discard db.collection("documents")

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
    check not docs[0].hasKey("email")

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

  test "Project with exclude - single field":
    let projection = %*{
      "password": 0
    }
    let docs = db.query().project(projection).list()
    check docs.len == 2
    check docs[0].hasKey("name")
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


suite "SimpleDB Extended Aggregation":
  var db: SimpleDB

  setup:
    db = SimpleDB()
    db.open(":memory:")
    discard db.collection("documents")

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

  test "Aggregate with avg":
    let result = db.aggregate("category", %*{ "$avg": "amount" })
    check result.len == 2

  test "Aggregate with min and max":
    let result = db.aggregate("category", %*{ "$min": "amount", "$max": "amount" })
    check result.len == 2

  test "Aggregate with multiple operators":
    let result = db.aggregate("category", %*{ "$sum": "amount", "$avg": "quantity" })
    check result.len == 2

  test "Aggregate with filter":
    let filter = %*{ "amount": { "$gte": 100 } }
    let result = db.aggregate("category", %*{ "$sum": "amount" }, filter)
    check result.len == 1


suite "SimpleDB Bulk Operations":
  var db: SimpleDB

  setup:
    db = SimpleDB()
    db.open(":memory:")
    discard db.collection("documents")

  teardown:
    db.close()

  test "Bulk insert documents":
    let docs = @[
      %*{ "id": "bulk-1", "name": "Item 1", "value": 10 },
      %*{ "id": "bulk-2", "name": "Item 2", "value": 20 },
      %*{ "id": "bulk-3", "name": "Item 3", "value": 30 }
    ]

    let inserted = db.bulkInsert(docs)
    check inserted == 3
    check db.query().count() == 3

  test "Bulk delete documents":
    for i in 1..10:
      db.put(%*{ "id": "del-" & $i, "name": "Item " & $i })

    let idsToDelete = @["del-2", "del-4", "del-6", "del-8", "del-10"]
    let deleted = db.bulkDelete(idsToDelete)
    check deleted == 5
    check db.query().count() == 5


suite "SimpleDB Aggregate Pipeline":
  var db: SimpleDB

  setup:
    db = SimpleDB()
    db.open(":memory:")
    discard db.collection("orders")

  teardown:
    db.close()

  test "Aggregate with $match and $group":
    db.put(%*{ "id": "o1", "customerId": "c1", "amount": 100, "status": "completed" })
    db.put(%*{ "id": "o2", "customerId": "c1", "amount": 200, "status": "completed" })
    db.put(%*{ "id": "o3", "customerId": "c2", "amount": 150, "status": "completed" })
    db.put(%*{ "id": "o4", "customerId": "c2", "amount": 50, "status": "pending" })

    let result = db.aggregate(@[
      %*{ "$match": { "status": "completed" } },
      %*{ "$group": { "_id": "$customerId", "total": { "$sum": "$amount" } } }
    ])

    check result.count == 2

  test "Aggregate with $sum: 1 (count)":
    db.put(%*{ "id": "o1", "customerId": "c1", "status": "completed" })
    db.put(%*{ "id": "o2", "customerId": "c1", "status": "completed" })
    db.put(%*{ "id": "o3", "customerId": "c2", "status": "completed" })

    let result = db.aggregate(@[
      %*{ "$group": { "_id": "$customerId", "orderCount": { "$sum": 1 } } }
    ])

    check result.count == 2

  test "Aggregate with $avg, $min, $max":
    db.put(%*{ "id": "o1", "customerId": "c1", "amount": 100 })
    db.put(%*{ "id": "o2", "customerId": "c1", "amount": 200 })
    db.put(%*{ "id": "o3", "customerId": "c1", "amount": 300 })

    let result = db.aggregate(@[
      %*{ "$group": { "_id": "$customerId", "avgAmount": { "$avg": "$amount" }, "minAmount": { "$min": "$amount" }, "maxAmount": { "$max": "$amount" } } }
    ])

    check result.count == 1

  test "Aggregate with $sort and $limit":
    db.put(%*{ "id": "o1", "customerId": "c1", "amount": 100 })
    db.put(%*{ "id": "o2", "customerId": "c2", "amount": 200 })
    db.put(%*{ "id": "o3", "customerId": "c3", "amount": 50 })
    db.put(%*{ "id": "o4", "customerId": "c4", "amount": 300 })

    let result = db.aggregate(@[
      %*{ "$group": { "_id": "$customerId", "total": { "$sum": "$amount" } } },
      %*{ "$sort": { "total": -1 } },
      %*{ "$limit": 2 }
    ])

    check result.count == 2

  test "Aggregate pipeline without $group":
    db.put(%*{ "id": "o1", "status": "completed", "priority": 3 })
    db.put(%*{ "id": "o2", "status": "pending", "priority": 1 })
    db.put(%*{ "id": "o3", "status": "completed", "priority": 5 })
    db.put(%*{ "id": "o4", "status": "completed", "priority": 2 })

    let result = db.aggregate(@[
      %*{ "$match": { "status": "completed" } },
      %*{ "$sort": { "priority": -1 } },
      %*{ "$limit": 2 }
    ])

    check result.count == 2

  test "Aggregate with complex $match filter":
    db.put(%*{ "id": "o1", "customerId": "c1", "amount": 100, "status": "completed" })
    db.put(%*{ "id": "o2", "customerId": "c1", "amount": 200, "status": "completed" })
    db.put(%*{ "id": "o3", "customerId": "c2", "amount": 50, "status": "pending" })
    db.put(%*{ "id": "o4", "customerId": "c2", "amount": 300, "status": "completed" })

    let result = db.aggregate(@[
      %*{ "$match": { "status": { "$ne": "pending" } } },
      %*{ "$group": { "_id": "$customerId", "total": { "$sum": "$amount" } } }
    ])

    check result.count == 2


# Clean up test database
removeFile("test.db")
