import classes
import std/json
import std/oids
import std/strutils
import std/sequtils
import db_connector/db_sqlite


##
## Helper: Convert field name to SQLite JSON path
## "user.name" -> "$.user.name"
## "tags[0]" -> "$.tags[0]"
proc toJsonPath(field: string): string =
    if field.len == 0:
        return "$"
    # If already starts with $., return as-is
    if field.startsWith("$."):
        return field
    # Otherwise, prepend $. for dot notation
    return "$." & field


##
## Query filter types
type FilterOp = enum
    foEq, foNe, foGt, foGte, foLt, foLte, foIn

type LogicalOp = enum
    loAnd, loOr

type ArrayOp = enum
    aoAll, aoElemMatch, aoSize

##
## Query filter info for a single condition
class SimpleDBFilter:
    var field = ""
    var operation = ""
    var value = ""
    var values: seq[string] = @[]  # For IN operations
    var fieldIsNumber = false

##
## Logical filter group for $and/$or operations
class SimpleDBLogicalFilter:
    var op: LogicalOp
    var filters: seq[SimpleDBFilter]

##
## Array filter for array operations ($all, $size)
class SimpleDBArrayFilter:
    var field = ""
    var op: ArrayOp
    var values: seq[string] = @[]  # For $all operation
    var size = 0  # For $size operation

##
## Exists filter for $exists operator
class SimpleDBExistsFilter:
    var field = ""
    var exists = true  # true = field exists, false = field does not exist


##
## Query builder
class SimpleDBQuery:

    ## Reference to the database
    var db: RootRef

    ## List of filters
    var filters : seq[SimpleDBFilter]

    ## List of logical filter groups ($and, $or)
    var logicalFilters: seq[SimpleDBLogicalFilter]

    ## List of array filters ($all, $size)
    var arrayFilters: seq[SimpleDBArrayFilter]

    ## List of exists filters ($exists)
    var existsFilters: seq[SimpleDBExistsFilter]

    ## Sort field
    var sortField = ""
    var sortAscending = true
    var sortIsNumber = true

    ## Limit
    var pLimit = -1

    ## Offset
    var pOffset = 0

    ## (chainable) Add a filter. Operation is one of: `==` `!=` `<` `<=` `>` `>=`
    method where(field: string, operation: string, value: string): SimpleDBQuery {.gcsafe.} =

        # Check input
        if field.len == 0: raiseAssert("No field provided")
        if operation.len == 0: raiseAssert("No operation provided")
        if operation != "==" and operation != "!=" and operation != "<" and operation != "<=" and operation != ">" and operation != ">=": raiseAssert("Unknown operation '" & operation & "'")

        # Add it
        let filter = SimpleDBFilter(field: field, operation: operation, value: value, fieldIsNumber: false)
        this.filters.add(filter)
        return this


    ## (chainable) Add a filter. Operation is one of: `==` `!=` `<` `<=` `>` `>=`
    method where(field: string, operation: string, value: float): SimpleDBQuery {.gcsafe.} =

        # Check input
        if field.len == 0: raiseAssert("No field provided")
        if operation.len == 0: raiseAssert("No operation provided")
        if operation != "==" and operation != "!=" and operation != "<" and operation != "<=" and operation != ">" and operation != ">=": raiseAssert("Unknown operation '" & operation & "'")

        # Add it
        let filter = SimpleDBFilter(field: field, operation: operation, value: $value, fieldIsNumber: true)
        this.filters.add(filter)
        return this
    

    ## (chainable) Add a filter from a JsonNode filter object (supports MongoDB-style $in)
    method filter(filterObj: JsonNode): SimpleDBQuery {.gcsafe.} =

        # Check input
        if filterObj == nil or filterObj.kind != JObject: 
            raiseAssert("Filter must be a JSON object")

        # Helper to convert JsonNode to string value
        proc jsonToString(node: JsonNode): string =
            case node.kind:
                of JString: return node.getStr()
                of JInt: return $node.getInt()
                of JFloat: return $node.getFloat()
                of JBool: return $node.getBool()
                else: return $node

        # Helper to process a single field condition and return a filter
        proc processCondition(field: string, val: JsonNode): SimpleDBFilter =
            if val.kind == JObject:
                # Check for MongoDB-style operators like $in
                if "$in" in val:
                    let inValues = val["$in"]
                    if inValues.kind == JArray and inValues.len > 0:
                        var values: seq[string] = @[]
                        for v in inValues:
                            values.add(jsonToString(v))
                        return SimpleDBFilter(field: field, operation: "IN", values: values, fieldIsNumber: false)
                elif "$eq" in val:
                    let eqVal = val["$eq"]
                    let isNum = eqVal.kind == JInt or eqVal.kind == JFloat
                    return SimpleDBFilter(field: field, operation: "==", value: jsonToString(eqVal), fieldIsNumber: isNum)
                elif "$ne" in val:
                    let neVal = val["$ne"]
                    let isNum = neVal.kind == JInt or neVal.kind == JFloat
                    return SimpleDBFilter(field: field, operation: "!=", value: jsonToString(neVal), fieldIsNumber: isNum)
                elif "$gt" in val:
                    let gtVal = val["$gt"]
                    let isNum = gtVal.kind == JInt or gtVal.kind == JFloat
                    return SimpleDBFilter(field: field, operation: ">", value: jsonToString(gtVal), fieldIsNumber: isNum)
                elif "$gte" in val:
                    let gteVal = val["$gte"]
                    let isNum = gteVal.kind == JInt or gteVal.kind == JFloat
                    return SimpleDBFilter(field: field, operation: ">=", value: jsonToString(gteVal), fieldIsNumber: isNum)
                elif "$lt" in val:
                    let ltVal = val["$lt"]
                    let isNum = ltVal.kind == JInt or ltVal.kind == JFloat
                    return SimpleDBFilter(field: field, operation: "<", value: jsonToString(ltVal), fieldIsNumber: isNum)
                elif "$lte" in val:
                    let lteVal = val["$lte"]
                    let isNum = lteVal.kind == JInt or lteVal.kind == JFloat
                    return SimpleDBFilter(field: field, operation: "<=", value: jsonToString(lteVal), fieldIsNumber: isNum)
            elif val.kind == JString:
                return SimpleDBFilter(field: field, operation: "==", value: val.getStr(), fieldIsNumber: false)
            elif val.kind == JInt or val.kind == JFloat:
                return SimpleDBFilter(field: field, operation: "==", value: $val, fieldIsNumber: true)
            elif val.kind == JBool:
                return SimpleDBFilter(field: field, operation: "==", value: $val.getBool(), fieldIsNumber: false)
            
            return SimpleDBFilter()

        # Check for $or operator
        if "$or" in filterObj:
            let orArray = filterObj["$or"]
            if orArray.kind == JArray:
                var orFilters: seq[SimpleDBFilter] = @[]
                for item in orArray:
                    if item.kind == JObject and item.len > 0:
                        # Get the first (and typically only) field from the object
                        for field, val in item:
                            let f = processCondition(field, val)
                            if f.field.len > 0:
                                orFilters.add(f)
                            break
                if orFilters.len > 0:
                    let logicalFilter = SimpleDBLogicalFilter(op: loOr, filters: orFilters)
                    this.logicalFilters.add(logicalFilter)

        # Check for $and operator
        if "$and" in filterObj:
            let andArray = filterObj["$and"]
            if andArray.kind == JArray:
                var andFilters: seq[SimpleDBFilter] = @[]
                for item in andArray:
                    if item.kind == JObject and item.len > 0:
                        for field, val in item:
                            let f = processCondition(field, val)
                            if f.field.len > 0:
                                andFilters.add(f)
                            break
                if andFilters.len > 0:
                    let logicalFilter = SimpleDBLogicalFilter(op: loAnd, filters: andFilters)
                    this.logicalFilters.add(logicalFilter)

        # Check for array operators and $exists in field conditions
        for field, val in filterObj:
            if field == "$or" or field == "$and":
                continue
            
            # Check for array operators and $exists
            if val.kind == JObject:
                var processedSpecialOp = false
                
                # Check for $all operator
                if "$all" in val:
                    let allValues = val["$all"]
                    if allValues.kind == JArray and allValues.len > 0:
                        var values: seq[string] = @[]
                        for v in allValues:
                            values.add(jsonToString(v))
                        let arrayFilter = SimpleDBArrayFilter(field: field, op: aoAll, values: values)
                        this.arrayFilters.add(arrayFilter)
                    processedSpecialOp = true
                
                # Check for $size operator
                if "$size" in val:
                    let sizeVal = val["$size"]
                    if sizeVal.kind == JInt:
                        let arrayFilter = SimpleDBArrayFilter(field: field, op: aoSize, size: sizeVal.getInt())
                        this.arrayFilters.add(arrayFilter)
                    processedSpecialOp = true
                
                # Check for $exists operator
                if "$exists" in val:
                    let existsVal = val["$exists"]
                    if existsVal.kind == JBool:
                        let existsFilter = SimpleDBExistsFilter(field: field, exists: existsVal.getBool())
                        this.existsFilters.add(existsFilter)
                    processedSpecialOp = true
                
                # If we processed any special operator, skip regular processing
                if processedSpecialOp:
                    continue
            
            # Process as regular condition
            let f = processCondition(field, val)
            if f.field.len > 0:
                this.filters.add(f)
        
        return this
    

    ## (chainable) Set sort field
    method sort(field: string, ascending: bool = true, isNumber: bool = true): SimpleDBQuery {.gcsafe.} =

        # Check input
        if field.len == 0: raiseAssert("No field provided")
        
        # Store it
        this.sortField = field
        this.sortAscending = ascending
        this.sortIsNumber = isNumber
        return this
    

    ## (chainable) Set the maximum number of documents to return, or -1 to return all documents.
    method limit(count: int): SimpleDBQuery {.gcsafe.} =

        # Check input
        if count < -1: raiseAssert("Cannot use negative numbers for the limit")

        # Store it
        this.pLimit = count
        return this


    ## (chainable) Set the number of documents to skip
    method offset(count: int): SimpleDBQuery {.gcsafe.} =

        # Check input
        if count < 0: raiseAssert("Cannot use negative numbers for the offset")

        # Store it
        this.pOffset = count
        return this




##
## A simple NoSQL database written in Nim.
class SimpleDB:

    ## (private) Database connection
    var conn : DbConn

    ## (private) True if the database has been prepared yet
    var hasPrepared = false

    ## (private) Extra columns that have been created for indexing
    var extraColumns: seq[string] = @["id_TEXT"]

    ## (private) List of hashes of generated indexes
    var createdIndexHashes: seq[string]

    ## Constructor
    method init(filename: string) {.gcsafe.} =

        # Create the database connection
        this.conn = open(filename, "", "", "")


    ## Close the database
    method close() {.gcsafe.} =

        # Close database
        if this.conn != nil:
            this.conn.close()
            this.conn = nil


    ## (private) Prepare the datatabase for use
    method prepareDB() {.gcsafe.} =

        # Only do once
        if this.hasPrepared: return
        this.hasPrepared = true

        # Create main table if it doesn't exist
        this.conn.exec(sql"CREATE TABLE IF NOT EXISTS documents (id_TEXT TEXT PRIMARY KEY, _json TEXT)")

        # Get list of all columns in the table
        for row in this.conn.rows(sql"PRAGMA table_info(documents)"):

            # Add to the extra columns array
            let columnName = row[1]
            if columnName == "_json": continue
            if not this.extraColumns.contains(columnName):
                this.extraColumns.add(columnName)


    ## Execute a batch of transactions. Either they all succeed, or the database will not be updated. This is also much faster when saving lots of documents at once.
    method batch(code: proc() {.gcsafe.}) {.gcsafe.} =

        # Prepate database
        this.prepareDB()

        # Start a transaction
        this.conn.exec sql"BEGIN TRANSACTION"

        # Catch errors
        try:

            # Execute the caller's code
            code()

        except:

            # Rollback the transaction
            this.conn.exec sql"ROLLBACK TRANSACTION"

            # Pass the error on to the caller
            raise getCurrentException()

        # Complete the transaction
        this.conn.exec sql"COMMIT TRANSACTION"


    ## Start a query
    method query(): SimpleDBQuery {.gcsafe.} =

        # Prepare database
        this.prepareDB()

        # Create query object
        let q = SimpleDBQuery.init()
        q.db = this
        return q


    ## (private) Ensure column exists for the specified field
    method createIndexableColumnForField(name: string, sqlName: string, sqlType: string) {.gcsafe.} =

        # Stop if already created
        if this.extraColumns.contains(sqlName):
            return

        # Begin an update transaction
        this.batch do():

            # Create new field on the table
            let str = "ALTER TABLE documents ADD \"" & sqlName & "\" " & sqlType
            this.conn.exec(sql(str))

            # Fetch all existing documents ... this is heavy, but we can't iterate and modify at the same time
            let sqlUpdateRow = sql("UPDATE documents SET \"" & sqlName & "\" = ? WHERE id_TEXT = ?")
            for row in this.conn.getAllRows(sql"SELECT id_TEXT, _json FROM documents"):

                # Parse this document
                let id = row[0]
                let json = parseJson(row[1])

                # Get field value
                let node = json{name}
                var value = ""
                if node.isNil: value = ""
                elif node.kind == JString: value = node.getStr()
                elif node.kind == JFloat: value = $node.getFloat()
                elif node.kind == JInt: value = $node.getInt()

                # Set row value
                if value.len > 0:
                    this.conn.exec(sqlUpdateRow, value, id)

        # Done, update extra columns
        this.extraColumns.add(sqlName)


    ## (private) Create an index for the specified query, if needed
    method createIndex(query: SimpleDBQuery) {.gcsafe.} =

        # Stop if no index is needed, ie this query returns all data directly
        if query.sortField == "" and query.filters.len == 0:
            return

        # Check if index created
        let indexHash = query.filters.mapIt(it.field).join("_") & query.sortField
        if this.createdIndexHashes.contains(indexHash):
            return
        
        # Create SQL
        var sqlStr = "CREATE INDEX IF NOT EXISTS \"documents_" & indexHash & "\" ON documents ("

        # Add filter fields
        var addedFirst = false
        for filter in query.filters:

            # Get SQL column info
            var sqlType = if filter.fieldIsNumber: "REAL" else: "TEXT"
            var sqlName = filter.field & "_" & sqlType
            
            # Add the separator if this is not the first filter
            if addedFirst: sqlStr &= ", "
            addedFirst = true

            # Add the filter
            sqlStr &= "\"" & sqlName & "\""

        # Add sort field
        if query.sortField.len > 0:

            # Get SQL column info
            var sqlType = if query.sortIsNumber: "REAL" else: "TEXT"
            var sqlName = query.sortField & "_" & sqlType
            
            # Add the separator if this is not the first filter
            if addedFirst: sqlStr &= ", "
            addedFirst = true

            # Add the filter
            sqlStr &= "\"" & sqlName & "\""

        # Close the SQL
        sqlStr &= ")"

        # Execute it
        this.conn.exec(sql(sqlStr))

        # Done, store index hash
        this.createdIndexHashes.add(indexHash)



## Helper: Build SQL condition for a single filter
proc buildFilterSql(filter: SimpleDBFilter, bindValues: var seq[string]): string =
    if filter.operation == "IN":
        # Handle IN operation with multiple values
        result &= "json_extract(_json, ?) IN ("
        bindValues.add(filter.field.toJsonPath())
        for i in 0 ..< filter.values.len:
            if i > 0: result &= ", "
            result &= "?"
            bindValues.add(filter.values[i])
        result &= ")"
    else:
        # For numeric comparisons, cast json_extract result to REAL
        if filter.fieldIsNumber:
            result &= "CAST(json_extract(_json, ?) AS REAL) " & filter.operation & " CAST(? AS REAL)"
        else:
            result &= "json_extract(_json, ?) " & filter.operation & " ?"
        bindValues.add(filter.field.toJsonPath())
        bindValues.add(filter.value)


## Helper: Build SQL condition for array filters
proc buildArrayFilterSql(arrayFilter: SimpleDBArrayFilter, bindValues: var seq[string]): string =
    case arrayFilter.op:
        of aoAll:
            # $all - Array contains all specified values
            # Use json_each to check if all values exist in the array
            var conditions: seq[string] = @[]
            for value in arrayFilter.values:
                conditions.add("EXISTS (SELECT 1 FROM json_each(_json, ?) WHERE value = ?)")
                bindValues.add(arrayFilter.field.toJsonPath())
                bindValues.add(value)
            result &= conditions.join(" AND ")
        of aoSize:
            # $size - Array has specific length
            # Check if json_type is 'array' and length matches
            # Use CAST to ensure proper comparison
            result &= "json_type(json_extract(_json, ?)) = 'array' AND CAST(json_array_length(json_extract(_json, ?)) AS TEXT) = ?"
            bindValues.add(arrayFilter.field.toJsonPath())
            bindValues.add(arrayFilter.field.toJsonPath())
            bindValues.add($arrayFilter.size)
        of aoElemMatch:
            # $elemMatch - Not implemented yet (requires complex subquery)
            discard


## Helper: Build SQL condition for exists filters
proc buildExistsFilterSql(existsFilter: SimpleDBExistsFilter, bindValues: var seq[string]): string =
    # Note: SQLite's json_extract returns NULL for both non-existent fields AND null values
    # So $exists: true matches fields with non-null values
    # And $exists: false matches non-existent fields OR null values
    if existsFilter.exists:
        # Field exists with non-null value
        result &= "json_extract(_json, ?) IS NOT NULL"
    else:
        # Field does not exist or is null
        result &= "json_extract(_json, ?) IS NULL"
    bindValues.add(existsFilter.field.toJsonPath())


## Execute the query and return all documents.
proc prepareQuerySql(this: SimpleDBQuery, sqlPrefix: string): (string, seq[string]) =

    # Get database reference
    let db = cast[SimpleDB](this.db)
    
    # Build query
    var bindValues : seq[string]
    var sqlStr = sqlPrefix

    # Check if we have any filters
    let hasFilters = this.filters.len > 0 or this.logicalFilters.len > 0 or this.arrayFilters.len > 0 or this.existsFilters.len > 0

    # Add filters
    if hasFilters:

        # Add WHERE clause
        sqlStr &= " WHERE "
        var addedFirst = false

        # Add regular filters (implicit AND)
        for filter in this.filters:
            # Add the 'AND' if this is not the first filter
            if addedFirst: sqlStr &= " AND "
            addedFirst = true

            # Add the filter
            sqlStr &= buildFilterSql(filter, bindValues)

        # Add logical filters ($or, $and)
        for logicalFilter in this.logicalFilters:
            if addedFirst: sqlStr &= " AND "
            addedFirst = true

            let logicalOp = if logicalFilter.op == loOr: " OR " else: " AND "
            sqlStr &= "("
            var firstInGroup = true
            for filter in logicalFilter.filters:
                if not firstInGroup: sqlStr &= logicalOp
                firstInGroup = false
                sqlStr &= buildFilterSql(filter, bindValues)
            sqlStr &= ")"

        # Add array filters ($all, $size)
        for arrayFilter in this.arrayFilters:
            if addedFirst: sqlStr &= " AND "
            addedFirst = true
            sqlStr &= buildArrayFilterSql(arrayFilter, bindValues)

        # Add exists filters ($exists)
        for existsFilter in this.existsFilters:
            if addedFirst: sqlStr &= " AND "
            addedFirst = true
            sqlStr &= buildExistsFilterSql(existsFilter, bindValues)
            
        # Add sort
        if this.sortField.len > 0:

            # Get SQL column info
            var sqlType = if this.sortIsNumber: "REAL" else: "TEXT"
            var sqlName = this.sortField & "_" & sqlType

            # Ensure an indexable column exists for this field
            db.createIndexableColumnForField(this.sortField, sqlName, sqlType)
            
            # Add the sort
            sqlStr &= " ORDER BY \"" & sqlName & "\" " & (if this.sortAscending: "asc" else: "desc")
            
        # Add limit (required before OFFSET in SQLite)
        if this.pLimit >= 0:
            sqlStr &= " LIMIT " & $this.pLimit
        elif this.pOffset > 0:
            # SQLite requires LIMIT when using OFFSET
            sqlStr &= " LIMIT -1"

        # Add offset
        if this.pOffset > 0:
            sqlStr &= " OFFSET " & $this.pOffset

        # Create index for this query if needed
        db.createIndex(this)

        # Done, prepare and bind the query
        return (sqlStr, bindValues)


## Execute the query and return all documents.
proc list*(this: SimpleDBQuery): seq[JsonNode] =

    # Get database reference
    let db = cast[SimpleDB](this.db)

    # Prepare the query
    let (sqlStr, bindValues) = prepareQuerySql(this, "SELECT _json FROM documents")

    # Run the query
    var docs : seq[JsonNode]
    for row in db.conn.rows(sql(sqlStr), bindValues):

        # Parse JSON for each result
        docs.add(parseJson(row[0]))

    # Done
    return docs


## Execute the query and iterate through the resulting documents.
iterator list*(this: SimpleDBQuery): JsonNode =

    # Get database reference
    let db = cast[SimpleDB](this.db)

    # Prepare the query
    let (sqlStr, bindValues) = prepareQuerySql(this, "SELECT _json FROM documents")

    # Run the query
    for row in db.conn.rows(sql(sqlStr), bindValues):

        # Parse JSON for each result and yield it
        yield parseJson(row[0])


## Execute the query and return the count of matching documents.
proc count*(this: SimpleDBQuery): int {.discardable.} =

    # Get database reference
    let db = cast[SimpleDB](this.db)

    # Prepare the query
    let (sqlStr, bindValues) = prepareQuerySql(this, "SELECT COUNT(*) FROM documents")

    # Run the query and get the count
    let countStr = db.conn.getValue(sql(sqlStr), bindValues)
    if countStr.len > 0:
        return parseInt(countStr)
    else:
        return 0


## Execute the query and return distinct values for a field using SQL DISTINCT.
proc distinctValues*(this: SimpleDBQuery, field: string): seq[string] {.gcsafe.} =

    # Get database reference
    let db = cast[SimpleDB](this.db)

    # Build SQL for distinct values using json_extract
    # Note: We need to handle the query filters but select distinct values
    let (whereSql, bindValues) = prepareQuerySql(this, "")
    
    # Build the full SQL with DISTINCT and json_extract
    var sqlStr = "SELECT DISTINCT json_extract(_json, '$.' || ?) FROM documents"
    var allBindValues = @[field]
    
    # Add WHERE clause if there are filters
    if whereSql.len > 0:
        sqlStr &= whereSql
        allBindValues.add(bindValues)

    # Run the query
    result = @[]
    for row in db.conn.rows(sql(sqlStr), allBindValues):
        if row[0].len > 0:
            result.add(row[0])


## Remove the documents matched by this query.
proc remove*(this: SimpleDBQuery): int {.discardable.} =

    # Get database reference
    let db = cast[SimpleDB](this.db)

    # Prepare the query
    let (sqlStr, bindValues) = prepareQuerySql(this, "DELETE FROM documents")

    # Run the query
    return int db.conn.execAffectedRows(sql(sqlStr), bindValues)


## Execute the query and return the first document found, or null if not found.
proc get*(this: SimpleDBQuery): JsonNode =

    # Limit to one
    this.pLimit = 1

    # Execute query
    let docs = this.list()
    if docs.len == 0:
        return nil
    else:
        return docs[0]


## Helper: Get a document with the specified ID, or return nil if not found
proc get*(this: SimpleDB, id: string): JsonNode =
    return this.query().where("id", "==", id).get()


## Helper: Remove a document with the specified ID. Returns true if the document was removed, or false if no document was found with this ID.
proc remove*(this: SimpleDB, id: string): bool {.discardable.} = 
    let numRemoved = this.query().where("id", "==", id).limit(1).remove()
    return if numRemoved > 0: true else: false


## Update the documents matched by this query with the given fields. Returns the number of documents updated.
## Supports MongoDB-style $set operator: {"$set": {"field": value}}
proc update*(this: SimpleDBQuery, updates: JsonNode): int {.discardable.} =

    # Check input
    if updates == nil: raiseAssert("Cannot update with null document.")
    if updates.kind != JObject: raiseAssert("Updates must be an object.")
    if updates.len == 0: return 0

    # Get database reference
    let db = cast[SimpleDB](this.db)

    # Extract $set if present (MongoDB-style)
    var fieldsToUpdate = updates
    if "$set" in updates:
        fieldsToUpdate = updates["$set"]
        if fieldsToUpdate.kind != JObject:
            raiseAssert("$set value must be an object")
    
    # Build SET clause with json_set for each field
    var jsonSetExpr = "_json"
    
    for key, value in fieldsToUpdate.pairs:
        # Skip id field updates
        if key == "id": continue
        
        # Build json_set expression - use $$ to escape $ for strutils.% operator
        let jsonValue = if value.kind == JString: "\"" & value.getStr() & "\""
                        else: $value
        let jsonPath = "$$.$1" % key
        jsonSetExpr = "json_set(" & jsonSetExpr & ", '" & jsonPath & "', " & jsonValue & ")"

    # Build the full SQL
    var sqlStr = "UPDATE documents SET _json = " & jsonSetExpr
    
    # Add WHERE clause using json_extract
    var bindValues: seq[string] = @[]
    if this.filters.len > 0:
        sqlStr &= " WHERE "
        var addedFirst = false
        for filter in this.filters:
            if addedFirst: sqlStr &= " AND "
            addedFirst = true
            
            sqlStr &= "json_extract(_json, '$.' || ?) " & filter.operation & " ?"
            bindValues.add(filter.field)
            bindValues.add(filter.value)
    
    # Execute the query with only filter bind values (no set values needed for json_set)
    return int db.conn.execAffectedRows(sql(sqlStr), bindValues)


## Helper: Update a document with the specified ID. Returns true if the document was updated, or false if no document was found with this ID.
proc update*(this: SimpleDB, id: string, updates: JsonNode): bool {.discardable.} =
    let numUpdated = this.query().where("id", "==", id).limit(1).update(updates)
    return if numUpdated > 0: true else: false

## Aggregate documents by a field and count them using SQL GROUP BY
## Returns a sequence of JsonNode with {"_id": fieldValue, "count": count}
proc aggregateCount*(this: SimpleDB, collection: string, groupField: string, matchFilter: JsonNode = nil): seq[JsonNode] {.gcsafe.} =
    # Prepare database
    this.prepareDB()
    
    # Build base query with collection filter
    var query = this.query().where("_collection", "==", collection)
    
    # Apply additional filters if provided
    if matchFilter != nil and matchFilter.len > 0:
        query = query.filter(matchFilter)
    
    # Get the WHERE clause SQL
    let (whereSql, bindValues) = prepareQuerySql(query, "")
    
    # Build aggregate SQL using json_extract for GROUP BY
    var sqlStr = "SELECT json_extract(_json, '$.' || ?), COUNT(*) FROM documents"
    var allBindValues = @[groupField]
    
    # Add WHERE clause if there are filters
    if whereSql.len > 0:
        sqlStr &= whereSql
        allBindValues.add(bindValues)
    
    # Add GROUP BY
    sqlStr &= " GROUP BY json_extract(_json, '$.' || ?)"
    allBindValues.add(groupField)
    
    # Execute query
    result = @[]
    for row in this.conn.rows(sql(sqlStr), allBindValues):
        if row[0].len > 0:
            result.add(%*{"_id": row[0], "count": parseInt(row[1])})

## Put a new document into the database, or replace it if it already exists
proc writeDocument(this: SimpleDB, document: JsonNode) =

    # Check input
    if document == nil: raiseAssert("Cannot put a null document into the database.")
    if document.kind != JObject: raiseAssert("Document must be an object.")
    if document{"id"}.isNil: document["id"] = % $genOid()
    if document{"id"}.kind != JString: raiseAssert("ID must be a string.")

    # Prepare database
    this.prepareDB()

    # Create query including all fields
    let str = "INSERT OR REPLACE INTO documents (_json, " & this.extraColumns.join(", ") & ") VALUES (?, " & this.extraColumns.mapIt("?").join(", ") & ")"
    let cmd = sql(str)

    # First field is the JSON content
    var args = @[ $document ]

    # Add fields for the extra columns
    for columnName in this.extraColumns:

        # Get field name by removing the sql suffix
        let fieldName = columnName.substr(0, columnName.len - 6)

        # Add it
        args.add document{fieldName}.getStr()

    # Bind and execute the query
    this.conn.exec(cmd, args)


## Put a new document into the database, merging the fields if it already exists
proc put*(this: SimpleDB, document: JsonNode, merge: bool = false) =

    # If not merging, just write it
    if not merge:
        this.writeDocument(document)

    # Check input
    if document == nil: raiseAssert("Cannot put a null document into the database.")
    if document.kind != JObject: raiseAssert("Document must be an object.")
    if document{"id"}.isNil: 
        this.writeDocument(document)
        return
    if document{"id"}.kind != JString: raiseAssert("ID must be a string.")

    # Get existing document, or just save it normally if not found
    let id = document["id"].getStr()
    var existingDoc = this.get(id)
    if existingDoc == nil:
        this.writeDocument(document)
        return
        
    # Merge new fields
    for key, value in document.pairs:
        existingDoc[key] = value

    # Write it
    this.writeDocument(existingDoc)