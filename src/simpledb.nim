import std/[json, oids, strutils, sequtils]
import db_connector/db_sqlite
import classes


##
## Exception types for SimpleDB
type SimpleDBError* = object of CatchableError
    ## Base exception for all SimpleDB errors

type ValidationError* = object of SimpleDBError
    ## Raised when input validation fails

type DocumentError* = object of SimpleDBError
    ## Raised when document operations fail


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
    var fieldIsBoolean = false  # For boolean comparisons

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
## Type filter for $type operator
class SimpleDBTypeFilter:
    var field = ""
    var jsonType = ""  # "string", "number", "boolean", "array", "object", "null"

##
## Regex filter for $regex operator
class SimpleDBRegexFilter:
    var field = ""
    var pattern = ""
    var options = ""  # "i" for case-insensitive (using GLOB)


##
## Shared validation logic for where methods
proc validateWhereParams(field, operation: string) {.inline.} =
    if field.len == 0:
        raise newException(ValidationError, "No field provided")
    if operation.len == 0:
        raise newException(ValidationError, "No operation provided")
    if operation notin ["==", "!=", "<", "<=", ">", ">="]:
        raise newException(ValidationError, "Unknown operation: " & operation)

##
## Query builder
class SimpleDBQuery:

    ## Reference to the database
    ## Note: Using pointer to avoid circular dependency with SimpleDB
    ## The query() method allocates a stable copy of SimpleDB on the heap
    var db: pointer

    ## List of filters
    var filters : seq[SimpleDBFilter]

    ## List of logical filter groups ($and, $or)
    var logicalFilters: seq[SimpleDBLogicalFilter]

    ## List of array filters ($all, $size)
    var arrayFilters: seq[SimpleDBArrayFilter]

    ## List of exists filters ($exists)
    var existsFilters: seq[SimpleDBExistsFilter]

    ## List of type filters ($type)
    var typeFilters: seq[SimpleDBTypeFilter]

    ## List of regex filters ($regex)
    var regexFilters: seq[SimpleDBRegexFilter]

    ## Projection fields (field selection)
    var projection: JsonNode = nil  # nil means no projection (return all fields)
    var projectionInclude = true    # true = include specified fields, false = exclude specified fields

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
        validateWhereParams(field, operation)
        let filter = SimpleDBFilter(field: field, operation: operation, value: value, fieldIsNumber: false)
        this.filters.add(filter)
        return this

    ## (chainable) Add a filter. Operation is one of: `==` `!=` `<` `<=` `>` `>=`
    method where(field: string, operation: string, value: float): SimpleDBQuery {.gcsafe.} =
        validateWhereParams(field, operation)
        let filter = SimpleDBFilter(field: field, operation: operation, value: $value, fieldIsNumber: true)
        this.filters.add(filter)
        return this
    

    ## (chainable) Add a filter from a JsonNode filter object (supports MongoDB-style $in)
    method filter(filterObj: JsonNode): SimpleDBQuery {.gcsafe.} =

        # Check input
        if filterObj == nil or filterObj.kind != JObject:
            raise newException(ValidationError, "Filter must be a JSON object")

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
                # SQLite stores booleans as integers (1/0)
                let boolValue = if val.getBool(): "1" else: "0"
                return SimpleDBFilter(field: field, operation: "==", value: boolValue, fieldIsNumber: true, fieldIsBoolean: true)

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
                
                # Check for $type operator
                if "$type" in val:
                    let typeVal = val["$type"]
                    if typeVal.kind == JString:
                        let typeFilter = SimpleDBTypeFilter(field: field, jsonType: typeVal.getStr())
                        this.typeFilters.add(typeFilter)
                    processedSpecialOp = true
                
                # Check for $regex operator
                if "$regex" in val:
                    let regexVal = val["$regex"]
                    var pattern = ""
                    var options = ""
                    
                    if regexVal.kind == JString:
                        pattern = regexVal.getStr()
                    elif regexVal.kind == JObject:
                        if "$regex" in regexVal:
                            let patNode = regexVal["$regex"]
                            if patNode.kind == JString:
                                pattern = patNode.getStr()
                        if "$options" in regexVal:
                            let optNode = regexVal["$options"]
                            if optNode.kind == JString:
                                options = optNode.getStr()
                    
                    if pattern.len > 0:
                        let regexFilter = SimpleDBRegexFilter(field: field, pattern: pattern, options: options)
                        this.regexFilters.add(regexFilter)
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
        if field.len == 0:
            raise newException(ValidationError, "No field provided")
        
        # Store it
        this.sortField = field
        this.sortAscending = ascending
        this.sortIsNumber = isNumber
        return this
    

    ## (chainable) Set the maximum number of documents to return, or -1 to return all documents.
    method limit(count: int): SimpleDBQuery {.gcsafe.} =

        # Check input
        if count < -1:
            raise newException(ValidationError, "Cannot use negative numbers for the limit")

        # Store it
        this.pLimit = count
        return this


    ## (chainable) Set the number of documents to skip
    method offset(count: int): SimpleDBQuery {.gcsafe.} =

        # Check input
        if count < 0:
            raise newException(ValidationError, "Cannot use negative numbers for the offset")

        # Store it
        this.pOffset = count
        return this


    ## (chainable) Set projection to return only specific fields.
    ## Use { "field": 1 } to include fields, { "field": 0 } to exclude fields.
    ## Cannot mix include and exclude (except _id can always be excluded).
    method project(projectionObj: JsonNode): SimpleDBQuery {.gcsafe.} =

        # Check input
        if projectionObj == nil or projectionObj.kind != JObject:
            raise newException(ValidationError, "Projection must be a JSON object")

        # Validate projection - check for mixed include/exclude
        var hasInclude = false
        var hasExclude = false
        for field, val in projectionObj:
            if val.kind == JInt:
                if val.getInt() == 1:
                    hasInclude = true
                elif val.getInt() == 0:
                    hasExclude = true
        
        if hasInclude and hasExclude:
            raise newException(ValidationError, "Cannot mix include and exclude in projection (except _id)")

        # Store projection
        this.projection = projectionObj
        this.projectionInclude = hasInclude
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

    ## Open a database connection
    ##
    ## Parameters:
    ##   filename: Path to the SQLite database file. Use ":memory:" for an in-memory database.
    ##
    ## Example:
    ##   var db = SimpleDB()
    ##   db.open("mydb.db")
    ##   var memDb = SimpleDB()
    ##   memDb.open(":memory:")
    method open(filename: string) {.gcsafe.} =

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
        # Allocate a stable copy of SimpleDB on the heap
        # This avoids the cast from RootRef and ensures the pointer remains valid
        var dbCopy = cast[ptr SimpleDB](alloc0(sizeof(SimpleDB)))
        dbCopy[] = this
        q.db = dbCopy
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
        elif filter.fieldIsBoolean:
            # For boolean comparisons, SQLite stores booleans as integers (1/0)
            # Use numeric comparison
            result &= "CAST(json_extract(_json, ?) AS INTEGER) " & filter.operation & " CAST(? AS INTEGER)"
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


## Helper: Convert MongoDB-style type names to SQLite json_type values
proc toJsonTypeName(mongoType: string): seq[string] =
    # SQLite json_type returns: 'null', 'true', 'false', 'integer', 'real', 'text', 'array', 'object'
    # MongoDB types: "string", "number", "boolean", "array", "object", "null"
    case mongoType:
        of "string": return @["text"]
        of "number": return @["integer", "real"]
        of "boolean": return @["true", "false"]
        of "array": return @["array"]
        of "object": return @["object"]
        of "null": return @["null"]
        else: return @[]


## Helper: Build SQL condition for type filters
proc buildTypeFilterSql(typeFilter: SimpleDBTypeFilter, bindValues: var seq[string]): string =
    let sqliteTypes = toJsonTypeName(typeFilter.jsonType)
    if sqliteTypes.len == 0:
        return "1=1"  # Invalid type, match everything
    
    # Note: Use json_type(_json, path) directly, not json_type(json_extract(...))
    # The latter causes "malformed JSON" errors in SQLite
    if sqliteTypes.len == 1:
        result &= "json_type(_json, ?) = ?"
        bindValues.add(typeFilter.field.toJsonPath())
        bindValues.add(sqliteTypes[0])
    else:
        # Multiple SQLite types (e.g., number = integer OR real)
        result &= "json_type(_json, ?) IN ("
        bindValues.add(typeFilter.field.toJsonPath())
        for i in 0 ..< sqliteTypes.len:
            if i > 0: result &= ", "
            result &= "?"
            bindValues.add(sqliteTypes[i])
        result &= ")"


## Helper: Convert regex pattern to SQL LIKE pattern
## Note: This is a basic conversion. Complex regex features are not supported.
## Supports: .* -> %, . -> _, * -> %, ? -> _
proc regexToLikePattern(regexPattern: string): string =
    result = regexPattern
    
    # Handle anchors first
    let hasStartAnchor = result.startsWith("^")
    let hasEndAnchor = result.endsWith("$")
    
    if hasStartAnchor:
        result = result[1..^1]
    if hasEndAnchor and result.len > 0:
        result = result[0..^2]
    
    # Replace common regex patterns with LIKE patterns
    # .* -> % (any characters)
    result = result.replace(".*", "%")
    # . -> _ (single character)
    result = result.replace(".", "_")
    # Also support shell-style wildcards
    # * -> % (any characters)
    result = result.replace("*", "%")
    # ? -> _ (single character)
    result = result.replace("?", "_")
    
    # If no start anchor, match anywhere
    if not hasStartAnchor and not result.startsWith("%"):
        result = "%" & result
    
    # If no end anchor, match anywhere
    if not hasEndAnchor and not result.endsWith("%"):
        result = result & "%"


## Helper: Build SQL condition for regex filters
proc buildRegexFilterSql(regexFilter: SimpleDBRegexFilter, bindValues: var seq[string]): string =
    # Use LIKE for basic regex support (case-insensitive with options="i")
    # GLOB is case-sensitive by default, LIKE is case-insensitive by default in SQLite
    # We use LIKE for case-insensitive, GLOB for case-sensitive
    
    let likePattern = regexToLikePattern(regexFilter.pattern)
    
    # Check if case-insensitive is requested
    let caseInsensitive = "i" in regexFilter.options
    
    if caseInsensitive or regexFilter.options.len == 0:
        # Use LIKE (case-insensitive by default in SQLite)
        result &= "json_extract(_json, ?) LIKE ? ESCAPE '\\'"
    else:
        # Use GLOB (case-sensitive)
        result &= "json_extract(_json, ?) GLOB ?"
    
    bindValues.add(regexFilter.field.toJsonPath())
    bindValues.add(likePattern)


## Execute the query and return all documents.
proc prepareQuerySql(this: SimpleDBQuery, sqlPrefix: string): (string, seq[string]) =

    # Get database reference
    let db = cast[ptr SimpleDB](this.db)[]
    
    # Build query
    var bindValues : seq[string]
    var sqlStr = sqlPrefix

    # Check if we have any filters
    let hasFilters = this.filters.len > 0 or this.logicalFilters.len > 0 or this.arrayFilters.len > 0 or this.existsFilters.len > 0 or this.typeFilters.len > 0 or this.regexFilters.len > 0

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
        
        # Add type filters ($type)
        for typeFilter in this.typeFilters:
            if addedFirst: sqlStr &= " AND "
            addedFirst = true
            sqlStr &= buildTypeFilterSql(typeFilter, bindValues)
        
        # Add regex filters ($regex)
        for regexFilter in this.regexFilters:
            if addedFirst: sqlStr &= " AND "
            addedFirst = true
            sqlStr &= buildRegexFilterSql(regexFilter, bindValues)
            
    # Add sort (applies with or without filters)
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


## Helper: Apply projection to a document
proc applyProjection(doc: JsonNode, projection: JsonNode, includeMode: bool): JsonNode =
    if projection == nil or projection.kind != JObject:
        return doc

    if includeMode:
        # Include mode: only include specified fields
        var projected = newJObject()
        for field, val in projection:
            if val.getInt() == 1:
                # Handle nested fields with dot notation
                let parts = field.split('.')
                var currentDoc = doc
                var currentResult = projected
                var found = true

                for i, part in parts:
                    if currentDoc.hasKey(part):
                        if i == parts.len - 1:
                            # Last part - copy the value
                            currentResult[part] = currentDoc[part]
                        else:
                            # Navigate deeper
                            currentDoc = currentDoc[part]
                            if not currentResult.hasKey(part):
                                currentResult[part] = newJObject()
                            currentResult = currentResult[part]
                    else:
                        found = false
                        break

                # If not found as nested, try as flat field
                if not found and doc.hasKey(field):
                    projected[field] = doc[field]
        return projected
    else:
        # Exclude mode: copy all fields except excluded ones
        result = doc.copy()
        for field, val in projection:
            if val.getInt() == 0:
                # Handle nested fields - for now just delete top-level
                let parts = field.split('.')
                if parts.len == 1:
                    if result.hasKey(field):
                        result.delete(field)
                else:
                    # For nested fields, we'd need more complex logic
                    # For now, just delete the top-level key if it matches
                    if result.hasKey(parts[0]):
                        var shouldDelete = true
                        var current = result[parts[0]]
                        for i in 1 ..< parts.len:
                            if current.kind == JObject and current.hasKey(parts[i]):
                                current = current[parts[i]]
                            else:
                                shouldDelete = false
                                break
                        # Note: Full nested deletion is complex, skip for now
        return result


## Helper: Check if projection has nested fields (contains dots)
proc hasNestedFields(this: SimpleDBQuery): bool =
    if this.projection == nil:
        return false
    for field, val in this.projection:
        if val.getInt() == 1 and field.contains('.'):
            return true
    return false


## Helper: Build SQL SELECT clause for projection
proc buildProjectionSql(this: SimpleDBQuery): string =
    ## Builds SQL SELECT clause using json_extract for include projections
    ## Returns "SELECT _json" if no projection, exclude mode, or nested fields
    ## Returns "SELECT json_object(...)" for flat include mode projections
    
    if this.projection == nil or not this.projectionInclude:
        # No projection or exclude mode - fetch full document
        return "SELECT _json FROM documents"
    
    # For nested fields, fall back to in-memory projection
    if this.hasNestedFields():
        return "SELECT _json FROM documents"
    
    # Include mode with only flat fields - build json_object
    var extracts: seq[string] = @[]
    for field, val in this.projection:
        if val.getInt() == 1:
            # Build json_extract for this field
            let jsonPath = toJsonPath(field)
            extracts.add("'" & field & "', json_extract(_json, '" & jsonPath & "')")
    
    if extracts.len == 0:
        # No valid fields to include
        return "SELECT _json FROM documents"
    
    return "SELECT json_object(" & extracts.join(", ") & ") FROM documents"


## Execute the query and return all documents.
proc list*(this: SimpleDBQuery): seq[JsonNode] =
    ## Returns all documents matching the query
    ## 
    ## Example:
    ##   let docs = db.query().where("status", "==", "active").list()
    
    let db = cast[ptr SimpleDB](this.db)[]
    
    # Build SELECT clause based on projection
    let selectPrefix = this.buildProjectionSql()
    let (sqlStr, bindValues) = prepareQuerySql(this, selectPrefix)

    for row in db.conn.rows(sql(sqlStr), bindValues):
        var doc = parseJson(row[0])
        # Apply in-memory projection for:
        # - Exclude mode (always done in memory)
        # - Include mode with nested fields (fall back to in-memory)
        if this.projection != nil and (not this.projectionInclude or this.hasNestedFields()):
            doc = applyProjection(doc, this.projection, this.projectionInclude)
        result.add(doc)


## Execute the query and iterate through the resulting documents.
iterator list*(this: SimpleDBQuery): JsonNode =

    # Get database reference
    let db = cast[ptr SimpleDB](this.db)[]

    # Build SELECT clause based on projection
    let selectPrefix = this.buildProjectionSql()
    let (sqlStr, bindValues) = prepareQuerySql(this, selectPrefix)

    # Run the query
    for row in db.conn.rows(sql(sqlStr), bindValues):

        # Parse JSON for each result
        var doc = parseJson(row[0])

        # Apply in-memory projection for:
        # - Exclude mode (always done in memory)
        # - Include mode with nested fields (fall back to in-memory)
        if this.projection != nil and (not this.projectionInclude or this.hasNestedFields()):
            doc = applyProjection(doc, this.projection, this.projectionInclude)

        # Yield the document
        yield doc


## Execute the query and return the query plan for analysis.
## Returns a sequence of JsonNode with query plan details.
proc explain*(this: SimpleDBQuery): seq[JsonNode] =

    # Get database reference
    let db = cast[ptr SimpleDB](this.db)[]

    # Prepare the query SQL (but we don't need bind values for EXPLAIN)
    let (sqlStr, bindValues) = prepareQuerySql(this, "SELECT _json FROM documents")

    # Build EXPLAIN QUERY PLAN SQL
    let explainSql = "EXPLAIN QUERY PLAN " & sqlStr

    # Run the explain query
    result = @[]
    for row in db.conn.rows(sql(explainSql), bindValues):
        # Each row contains: id, parent, notused, detail
        result.add(%*{
            "id": parseInt(row[0]),
            "parent": parseInt(row[1]),
            "notused": parseInt(row[2]),
            "detail": row[3]
        })


## Execute the query and return the count of matching documents.
proc count*(this: SimpleDBQuery): int {.discardable.} =

    # Get database reference
    let db = cast[ptr SimpleDB](this.db)[]

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
    let db = cast[ptr SimpleDB](this.db)[]

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
    let db = cast[ptr SimpleDB](this.db)[]

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


## Helper: Remove a document with the specified ID. Returns true if a document was removed.
proc removeOne*(this: SimpleDB, id: string): bool =
    return this.query().where("id", "==", id).limit(1).remove() > 0


## Update the documents matched by this query with the given fields. Returns the number of documents updated.
## Supports MongoDB-style operators:
##   $set: {"$set": {"field": value}} - Set field to value
##   $inc: {"$inc": {"field": value}} - Increment field by value (default 0 if not exists)
##   $mul: {"$mul": {"field": value}} - Multiply field by value (default 0 if not exists)
proc update*(this: SimpleDBQuery, updates: JsonNode): int {.discardable.} =

    # Check input
    if updates == nil:
        raise newException(ValidationError, "Cannot update with null document")
    if updates.kind != JObject:
        raise newException(ValidationError, "Updates must be an object")
    if updates.len == 0: return 0

    # Get database reference
    let db = cast[ptr SimpleDB](this.db)[]

    # Collect all json_set operations and their bind parameters
    var jsonSetPaths: seq[string] = @[]
    var jsonSetValues: seq[string] = @[]
    
    # Process $set operator
    if "$set" in updates:
        let fieldsToUpdate = updates["$set"]
        if fieldsToUpdate.kind != JObject:
            raise newException(ValidationError, "$set value must be an object")
        
        for key, value in fieldsToUpdate.pairs:
            # Skip id field updates
            if key == "id": continue
            
            # Collect path and value as bind parameters
            jsonSetPaths.add("$." & key)
            jsonSetValues.add($value)
    
    # Process $inc operator (increment)
    if "$inc" in updates:
        let fieldsToInc = updates["$inc"]
        if fieldsToInc.kind != JObject:
            raise newException(ValidationError, "$inc value must be an object")
        
        for key, value in fieldsToInc.pairs:
            # Skip id field updates
            if key == "id": continue
            
            # For $inc, we need to use json_extract in the expression
            # We'll handle this separately after building the base json_set
            let jsonPath = "$." & key
            let incValue = if value.kind == JString: value.getStr() else: $value
            # Store as special marker for increment operation
            jsonSetPaths.add(jsonPath & "||INC||" & incValue)
            jsonSetValues.add("")  # Placeholder
    
    # Process $mul operator (multiply)
    if "$mul" in updates:
        let fieldsToMul = updates["$mul"]
        if fieldsToMul.kind != JObject:
            raise newException(ValidationError, "$mul value must be an object")
        
        for key, value in fieldsToMul.pairs:
            # Skip id field updates
            if key == "id": continue
            
            let jsonPath = "$." & key
            let mulValue = if value.kind == JString: value.getStr() else: $value
            # Store as special marker for multiply operation
            jsonSetPaths.add(jsonPath & "||MUL||" & mulValue)
            jsonSetValues.add("")  # Placeholder
    
    # Process $unset operator (remove fields)
    var unsetPaths: seq[string] = @[]
    if "$unset" in updates:
        let fieldsToUnset = updates["$unset"]
        if fieldsToUnset.kind != JObject:
            raise newException(ValidationError, "$unset value must be an object")
        
        for key, value in fieldsToUnset.pairs:
            # Skip id field updates
            if key == "id": continue
            
            unsetPaths.add("$." & key)
    
    # Process $rename operator (rename fields)
    var renameOps: seq[tuple[oldPath, newPath: string]] = @[]
    if "$rename" in updates:
        let fieldsToRename = updates["$rename"]
        if fieldsToRename.kind != JObject:
            raise newException(ValidationError, "$rename value must be an object")
        
        for oldKey, newKeyNode in fieldsToRename.pairs:
            # Skip id field updates
            if oldKey == "id": continue
            if newKeyNode.kind != JString:
                raise newException(ValidationError, "$rename values must be strings")
            let newKey = newKeyNode.getStr()
            if newKey == "id":
                raise newException(ValidationError, "Cannot rename to 'id' field")
            
            renameOps.add(("$." & oldKey, "$." & newKey))

    # Build the SQL expression
    # Start with _json and apply json_set for each path
    var sqlExpr = "_json"
    var bindValues: seq[string] = @[]
    
    # Apply $set operations
    for i in 0..<jsonSetPaths.len:
        let path = jsonSetPaths[i]
        
        # Check if this is a special operation (INC or MUL)
        if "||INC||" in path:
            let parts = path.split("||INC||")
            let jsonPath = parts[0]
            let incValue = parts[1]
            # Use json_extract with COALESCE for increment
            sqlExpr = "json_set(" & sqlExpr & ", ?, COALESCE(json_extract(_json, ?), 0) + " & incValue & ")"
            bindValues.add(jsonPath)
            bindValues.add(jsonPath)
        elif "||MUL||" in path:
            let parts = path.split("||MUL||")
            let jsonPath = parts[0]
            let mulValue = parts[1]
            # Use json_extract with COALESCE for multiply
            sqlExpr = "json_set(" & sqlExpr & ", ?, COALESCE(json_extract(_json, ?), 0) * " & mulValue & ")"
            bindValues.add(jsonPath)
            bindValues.add(jsonPath)
        else:
            # Regular $set operation - use parameter for value
            sqlExpr = "json_set(" & sqlExpr & ", ?, json(?))"
            bindValues.add(path)
            bindValues.add(jsonSetValues[i])
    
    # Apply $unset operations (json_remove)
    for unsetPath in unsetPaths:
        sqlExpr = "json_remove(" & sqlExpr & ", ?)"
        bindValues.add(unsetPath)
    
    # Apply $rename operations
    for renameOp in renameOps:
        # First copy: json_set(..., '$.new', json_extract(_json, '$.old'))
        sqlExpr = "json_set(" & sqlExpr & ", ?, json_extract(_json, ?))"
        bindValues.add(renameOp.newPath)
        bindValues.add(renameOp.oldPath)
        # Then remove: json_remove(..., '$.old')
        sqlExpr = "json_remove(" & sqlExpr & ", ?)"
        bindValues.add(renameOp.oldPath)

    # Build the full SQL
    var sqlStr = "UPDATE documents SET _json = " & sqlExpr
    
    # Add WHERE clause using json_extract
    if this.filters.len > 0:
        sqlStr &= " WHERE "
        var addedFirst = false
        for filter in this.filters:
            if addedFirst: sqlStr &= " AND "
            addedFirst = true
            
            sqlStr &= "json_extract(_json, '$.' || ?) " & filter.operation & " ?"
            bindValues.add(filter.field)
            bindValues.add(filter.value)
    
    # Execute the query with all bind values
    return int db.conn.execAffectedRows(sql(sqlStr), bindValues)


## Helper: Update a document with the specified ID. Returns true if a document was updated.
proc updateOne*(this: SimpleDB, id: string, updates: JsonNode): bool =
    return this.query().where("id", "==", id).limit(1).update(updates) > 0

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
            result.add(%*{"id": row[0], "count": parseInt(row[1])})


## Aggregation result type
type AggregateResult* = object
    groupId*: string
    count*: int
    sum*: float
    avg*: float
    min*: float
    max*: float


## Extended aggregation with multiple operators
## Supports: $sum, $avg, $min, $max, $count
## Example: aggregate("category", "amount", %*{ "$sum": "amount", "$avg": "amount" })
proc aggregate*(this: SimpleDB, groupField: string, aggregations: JsonNode, matchFilter: JsonNode = nil): seq[AggregateResult] {.gcsafe.} =
    ## Performs aggregation with multiple operators
    ## groupField: Field to group by
    ## aggregations: Json object with aggregation operators
    ##   Example: %*{ "$sum": "amount", "$avg": "price", "$min": "stock" }
    ## matchFilter: Optional filter to apply before aggregation
    
    # Prepare database
    this.prepareDB()
    
    # Build base query
    var query = this.query()
    
    # Apply filter if provided
    if matchFilter != nil and matchFilter.len > 0:
        query = query.filter(matchFilter)
    
    # Get the WHERE clause SQL
    let (whereSql, bindValues) = prepareQuerySql(query, "")
    
    # Build SELECT clause
    var selectFields = @[
        "json_extract(_json, '$.' || ?) as _id",  # Group by field
        "COUNT(*) as count"
    ]
    var allBindValues = @[groupField]
    
    # Parse aggregation operators
    var hasSum = false
    var hasAvg = false
    var hasMin = false
    var hasMax = false
    var sumField = ""
    var avgField = ""
    var minField = ""
    var maxField = ""
    
    if aggregations != nil and aggregations.kind == JObject:
        if "$sum" in aggregations:
            hasSum = true
            sumField = aggregations["$sum"].getStr()
            selectFields.add("SUM(CAST(json_extract(_json, '$.' || ?) AS REAL)) as sum_val")
            allBindValues.add(sumField)
        
        if "$avg" in aggregations:
            hasAvg = true
            avgField = aggregations["$avg"].getStr()
            selectFields.add("AVG(CAST(json_extract(_json, '$.' || ?) AS REAL)) as avg_val")
            allBindValues.add(avgField)
        
        if "$min" in aggregations:
            hasMin = true
            minField = aggregations["$min"].getStr()
            selectFields.add("MIN(CAST(json_extract(_json, '$.' || ?) AS REAL)) as min_val")
            allBindValues.add(minField)
        
        if "$max" in aggregations:
            hasMax = true
            maxField = aggregations["$max"].getStr()
            selectFields.add("MAX(CAST(json_extract(_json, '$.' || ?) AS REAL)) as max_val")
            allBindValues.add(maxField)
    
    # Build full SQL
    var sqlStr = "SELECT " & selectFields.join(", ") & " FROM documents"
    
    # Add WHERE clause if there are filters
    if whereSql.len > 0:
        sqlStr &= whereSql
        allBindValues.add(bindValues)
    
    # Add GROUP BY
    sqlStr &= " GROUP BY json_extract(_json, '$.' || ?)"
    allBindValues.add(groupField)
    
    # Execute query
    result = @[]
    var colIndex = 0
    for row in this.conn.rows(sql(sqlStr), allBindValues):
        colIndex = 0
        var res = AggregateResult()
        res.groupId = row[colIndex]; colIndex += 1
        res.count = parseInt(row[colIndex]); colIndex += 1
        
        if hasSum:
            if row[colIndex].len > 0:
                res.sum = parseFloat(row[colIndex])
            colIndex += 1
        
        if hasAvg:
            if row[colIndex].len > 0:
                res.avg = parseFloat(row[colIndex])
            colIndex += 1
        
        if hasMin:
            if row[colIndex].len > 0:
                res.min = parseFloat(row[colIndex])
            colIndex += 1
        
        if hasMax:
            if row[colIndex].len > 0:
                res.max = parseFloat(row[colIndex])
            colIndex += 1
        
        result.add(res)


## Put a new document into the database, or replace it if it already exists
proc writeDocument(this: SimpleDB, document: JsonNode) =

    # Check input
    if document == nil:
        raise newException(DocumentError, "Cannot put a null document into the database")
    if document.kind != JObject:
        raise newException(DocumentError, "Document must be an object")

    # Create a copy to avoid modifying the input
    var docCopy = document.copy()

    # Generate ID if not provided
    if docCopy{"id"}.isNil:
        docCopy["id"] = % $genOid()
    if docCopy{"id"}.kind != JString:
        raise newException(DocumentError, "Document ID must be a string")

    # Prepare database
    this.prepareDB()

    # Create query including all fields
    let str = "INSERT OR REPLACE INTO documents (_json, " & this.extraColumns.join(", ") & ") VALUES (?, " & this.extraColumns.mapIt("?").join(", ") & ")"
    let cmd = sql(str)

    # First field is the JSON content
    var args = @[ $docCopy ]

    # Add fields for the extra columns
    for columnName in this.extraColumns:

        # Get field name by removing the sql suffix
        let fieldName = columnName.substr(0, columnName.len - 6)

        # Add it
        args.add docCopy{fieldName}.getStr()

    # Bind and execute the query
    this.conn.exec(cmd, args)


## Put a new document into the database, merging the fields if it already exists
proc put*(this: SimpleDB, document: JsonNode, merge: bool = false) =

    # If not merging, just write it and return
    if not merge:
        this.writeDocument(document)
        return

    # Check input
    if document == nil:
        raise newException(DocumentError, "Cannot put a null document into the database")
    if document.kind != JObject:
        raise newException(DocumentError, "Document must be an object")
    if document{"id"}.isNil: 
        this.writeDocument(document)
        return
    if document{"id"}.kind != JString:
        raise newException(DocumentError, "Document ID must be a string")

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


## Upsert a document: Update if it exists, insert if it doesn't.
## Returns the number of documents affected (always 1).
proc upsert*(this: SimpleDB, document: JsonNode): int {.discardable.} =

    # Check input
    if document == nil:
        raise newException(DocumentError, "Cannot upsert a null document into the database")
    if document.kind != JObject:
        raise newException(DocumentError, "Document must be an object")

    # Create a copy to avoid modifying the input
    var docCopy = document.copy()

    # Generate ID if not provided
    if docCopy{"id"}.isNil:
        docCopy["id"] = % $genOid()
    if docCopy{"id"}.kind != JString:
        raise newException(DocumentError, "Document ID must be a string")

    # Get the ID
    let id = docCopy["id"].getStr()

    # Check if document exists
    let existingDoc = this.get(id)
    if existingDoc == nil:
        # Document doesn't exist, insert it
        this.writeDocument(docCopy)
    else:
        # Document exists, update it (full replace)
        this.writeDocument(docCopy)
    return 1


## Upsert with merge: Update by merging fields if it exists, insert if it doesn't.
## Returns the number of documents affected (always 1).
proc upsert*(this: SimpleDB, document: JsonNode, merge: bool): int {.discardable.} =

    # If not merging, use the simple upsert
    if not merge:
        return this.upsert(document)

    # Check input
    if document == nil:
        raise newException(DocumentError, "Cannot upsert a null document into the database")
    if document.kind != JObject:
        raise newException(DocumentError, "Document must be an object")

    # Create a copy to avoid modifying the input
    var docCopy = document.copy()

    # Generate ID if not provided
    if docCopy{"id"}.isNil:
        docCopy["id"] = % $genOid()
    if docCopy{"id"}.kind != JString:
        raise newException(DocumentError, "Document ID must be a string")

    # Get the ID
    let id = docCopy["id"].getStr()

    # Check if document exists
    var existingDoc = this.get(id)
    if existingDoc == nil:
        # Document doesn't exist, insert it
        this.writeDocument(docCopy)
    else:
        # Document exists, merge new fields into existing document
        for key, value in docCopy.pairs:
            existingDoc[key] = value
        this.writeDocument(existingDoc)
    return 1

## Bulk insert multiple documents efficiently
## Returns the number of documents inserted
proc bulkInsert*(this: SimpleDB, documents: seq[JsonNode]): int {.discardable.} =
    ## Efficiently inserts multiple documents in a single transaction
    ## Much faster than individual put() calls for large datasets
    
    if documents.len == 0:
        return 0
    
    var count = 0
    this.batch do():
        for doc in documents:
            this.put(doc)
            count += 1
    return count


## Bulk delete multiple documents by ID
## Returns the number of documents deleted
proc bulkDelete*(this: SimpleDB, ids: seq[string]): int {.discardable.} =
    ## Efficiently deletes multiple documents by ID in a single transaction
    
    if ids.len == 0:
        return 0
    
    var count = 0
    this.batch do():
        for id in ids:
            if this.removeOne(id):
                count += 1
    return count
