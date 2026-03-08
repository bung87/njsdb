# NJSDB Code Review

## Executive Summary

NJSDB is a well-structured NoSQL-style document database built on SQLite. The codebase shows good understanding of Nim and SQLite JSON functions, but has several areas for improvement regarding idiomatic Nim usage, performance, memory safety, and API design.

---

## 1. Idiomatic Nim Usage

### 1.1 ✅ Good Practices

- **String interpolation**: Good use of `&` for string concatenation
- **Type safety**: Proper use of distinct types and enums
- **Optional parameters**: Good use of default parameter values

### 1.2 ⚠️ Issues Found

#### Issue 1.2.1: Unused Type Definitions
**Location**: Lines 25-32, 61

```nim
type FilterOp = enum  # UNUSED
    foEq, foNe, foGt, foGte, foLt, foLte, foIn
```

The `FilterOp` enum is defined but never used. The code uses string-based operations instead (`"=="`, `">="`, etc.).

**Recommendation**: Either use the enum type for the `operation` field or remove it.

```nim
# Option 1: Use the enum
class NJSDBFilter:
    var operation: FilterOp  # Instead of string

# Option 2: Remove the unused enum
type FilterOp = enum ...  # DELETE
```

#### Issue 1.2.2: ✅ FIXED: Inconsistent Error Handling
**Location**: Throughout
**Status**: **RESOLVED** - All `raiseAssert` calls replaced with `raise newException()`

The code previously mixed `raiseAssert` with `raise` for error handling. `raiseAssert` should only be used for programming errors, not runtime errors.

**Before**:
```nim
if field.len == 0: raiseAssert("No field provided")
```

**After**:
```nim
if field.len == 0:
    raise newException(ValidationError, "No field provided")
```

Now uses appropriate exception types:
- `ValidationError` for validation issues (invalid parameters, unknown operations)
- `DocumentError` for document-related issues (null documents, invalid document structure)

#### Issue 1.2.3: ⚠️ CANNOT FIX: Method vs Proc
**Location**: Throughout NJSDBQuery
**Status**: **BLOCKED** - The `classes` library requires `method` routines in class bodies

The code uses `method` for what should ideally be `proc`. In Nim, `method` implies dynamic dispatch (vtable-based), which is unnecessary here since there's no inheritance hierarchy.

**Current**:
```nim
method where(field: string, ...): NJSDBQuery {.gcsafe.} =
```

**Recommended**:
```nim
proc where*(field: string, ...): NJSDBQuery {.gcsafe.} =
```

**Limitation**: The `classes` library enforces that only `method` routines are allowed in class bodies. Attempting to use `proc` results in the error: "Only 'method' routines are allowed in the class body."

**Workaround**: This is a limitation of the `classes` library and cannot be changed without modifying the library itself or switching to a different OOP approach.

#### Issue 1.2.4: Boolean Expression Simplification
**Location**: Line 1032

```nim
return if numRemoved > 0: true else: false
```

Should be:
```nim
return numRemoved > 0
```

---

## 2. Performance Issues

### 2.1 🔴 Critical: SQL String Concatenation in Loops
**Location**: `prepareQuerySql` (lines 720-800)

The code builds SQL queries using string concatenation in loops, which is O(n²) due to string copying.

**Current**:
```nim
var sqlStr = sqlPrefix
# ...
for filter in this.filters:
    sqlStr &= " AND "  # Creates new string each time
    sqlStr &= buildFilterSql(filter, bindValues)
```

**Recommended**:
```nim
import std/strformat

var parts: seq[string]
parts.add(sqlPrefix)
# ...
for filter in this.filters:
    parts.add(" AND ")
    parts.add(buildFilterSql(filter, bindValues))
let sqlStr = parts.join("")
```

### 2.2 ✅ FIXED: SELECT * with Projection
**Location**: `list()` iterator and proc
**Status**: **RESOLVED** - Now uses SQL-level projection with `json_object()` and `json_extract()` for flat fields

The code previously always selected full documents (`SELECT _json`), even when projection was specified. Now it uses `json_extract` to select only needed fields for include projections with flat fields.

**Implementation**:
- Added `buildProjectionSql()` helper to generate SQL with `json_object()` for include projections
- Falls back to in-memory projection for nested fields and exclude mode
- Both `list()` proc and iterator updated

### 2.3 ⚠️ JSON Parsing on Every Row
**Location**: Lines 795, 823

Every row is parsed from JSON string to JsonNode. This is necessary for the current design but could be optimized with:
- Caching parsed documents
- Using a more efficient JSON parser
- Returning raw JSON strings for simple cases

### 2.4 ⚠️ Dynamic SQL Without Prepared Statements
**Location**: Throughout

While the code uses parameterized queries (good!), it rebuilds the SQL string for every query. For repeated queries with the same structure but different values, prepared statements would be more efficient.

---

## 3. Memory Safety Issues

### 3.1 ✅ FIXED: RootRef Cast
**Location**: Lines 82, 724
**Status**: **RESOLVED** - Now uses `pointer` with explicit type casting

```nim
# Before:
var db: RootRef  # In NJSDBQuery
# ...
let db = cast[NJSDB](this.db)  # Unsafe cast

# After:
var db: pointer  # In NJSDBQuery
# ...
let db = cast[ptr NJSDB](this.db)[]  # Explicit pointer cast
```

The `query()` method now allocates a stable heap copy of NJSDB to ensure the pointer remains valid.

### 3.2 ✅ FIXED: Special Variable Shadowing
**Location**: Line 934
**Status**: **RESOLVED** - Now uses implicit `result` variable directly

The code previously had a variable `docs` that shadowed Nim's implicit `result` variable.

**Before**:
```nim
proc list*(this: NJSDBQuery): seq[JsonNode] =
    var docs: seq[JsonNode]  # 'result' is implicitly declared
    # ...
    result = docs  # Shadowing warning
```

**After**:
```nim
proc list*(this: NJSDBQuery): seq[JsonNode] =
    # ...
    for row in db.conn.rows(sql(sqlStr), bindValues):
        var doc = parseJson(row[0])
        # ...
        result.add(doc)  # Uses implicit result directly
```

The `list()` procedure now properly uses Nim's implicit `result` variable without shadowing.

### 3.3 ⚠️ Nil JsonNode Checks
**Location**: Throughout

```nim
if document == nil: raiseAssert(...)
```

`JsonNode` should use `isNil` check, not `== nil`:
```nim
if document.isNil: raise newException(...)
```

### 3.4 ⚠️ Potential SQL Injection in Sort
**Location**: Lines 786-794

```nim
sqlStr &= " ORDER BY \"" & sqlName & "\" " & (if this.sortAscending: "asc" else: "desc")
```

While `sqlName` is constructed from internal data, there's no validation that field names are safe.

**Recommended**:
```nim
# Validate field names
proc isValidFieldName(name: string): bool =
    name.len > 0 and name.allIt(it in {'a'..'z', 'A'..'Z', '0'..'9', '_', '.'})
```

---

## 4. API Clarity Issues

### 4.1 ✅ FIXED: Inconsistent Return Types
**Location**: Throughout
**Status**: **RESOLVED** - Now uses consistent naming convention

Some procs returned `int` (count of affected rows), others returned `bool`. This was inconsistent.

```nim
# Before:
proc update*(...): int {.discardable.}  # Returns count
proc remove*(...): bool {.discardable.}  # Returns boolean (inconsistent!)

# After:
proc remove*(this: NJSDBQuery): int           # Always returns count
proc removeOne*(this: NJSDB, id: string): bool # Returns success for single doc
proc update*(this: NJSDBQuery): int 
proc updateOne*(this: NJSDB, id: string): bool
```

**Pattern**: Query versions return `int` (count), single-document `*One` versions return `bool` (success).

### 4.2 ✅ FIXED: Side Effects in Arguments
**Location**: Line 1427
**Status**: **RESOLVED** - No longer modifies input documents

```nim
# Before:
proc upsert*(this: NJSDB, document: JsonNode): bool =
    if document{"id"}.isNil: 
        document["id"] = % $genOid()  # Modifies input!

# After:
proc upsert*(this: NJSDB, document: JsonNode): int =
    var docCopy = document.copy()  # Create a copy
    if docCopy{"id"}.isNil:
        docCopy["id"] = % $genOid()
    # ... use docCopy instead of document
```

Fixed in `writeDocument`, `upsert`, and `put` procedures.

### 4.3 ⚠️ Missing Documentation
**Location**: Throughout

Many public procs lack proper documentation with examples. The `##` comments are good but could be more detailed.

### 4.4 ⚠️ Use of `discardable` Pragma
**Location**: Throughout

Overuse of `{.discardable.}` can hide bugs. Only use it when the return value is truly optional.

**Note**: The `*One` variants (returning `bool`) no longer have `{.discardable.}` to encourage checking the result.

---

## 5. Structural Issues

### 5.1 🔴 God Object Pattern
**Location**: NJSDBQuery class

The `NJSDBQuery` class has too many responsibilities:
- Query building
- Filter management
- SQL generation
- Result projection

**Recommendation**: Split into smaller types:
```nim
type
    QueryBuilder = object  # Build queries
    SqlGenerator = object  # Generate SQL
    QueryExecutor = object # Execute and fetch results
```

### 5.2 ✅ FIXED: Global State Risk
**Location**: Line 82
**Status**: **RESOLVED** - Now uses stable heap allocation

```nim
# Before:
var db: RootRef  # Reference to database

# After:
var db: pointer  # Points to heap-allocated copy
```

The `query()` method now allocates a stable copy on the heap using `alloc0()`.

### 5.3 ✅ PARTIALLY FIXED: Duplicate Code
**Location**: Lines 147-161
**Status**: **PARTIALLY RESOLVED** - Validation logic extracted, remaining duplication minimal

The two `where` methods (string and float) were nearly identical.

**Fix Applied**:
- Extracted validation logic to `validateWhereParams` procedure with `{.inline.}` pragma
- Both methods now share the same validation code

**Before**:
```nim
method where(field: string, operation: string, value: string): NJSDBQuery {.gcsafe.} =
    if field.len == 0: raise newException(ValidationError, "No field provided")
    if operation.len == 0: raise newException(ValidationError, "No operation provided")
    if operation notin ["==", "!=", "<", "<=", ">", ">="]:
        raise newException(ValidationError, "Unknown operation: " & operation)
    let filter = NJSDBFilter(field: field, operation: operation, value: value, fieldIsNumber: false)
    this.filters.add(filter)
    return this
```

**After**:
```nim
method where(field: string, operation: string, value: string): NJSDBQuery {.gcsafe.} =
    validateWhereParams(field, operation)
    let filter = NJSDBFilter(field: field, operation: operation, value: value, fieldIsNumber: false)
    this.filters.add(filter)
    return this
```

**Limitation**: The `classes` library only allows `method` and `var` declarations inside class bodies. Templates and helper procs cannot be used inside the class, preventing further deduplication of the filter creation logic.

### 5.4 ⚠️ Feature Envy
**Location**: SQL building procs

Procs like `buildFilterSql`, `buildArrayFilterSql` take filters but could be methods on the filter types.

**Recommended**:
```nim
class NJSDBFilter:
    proc toSql(bindValues: var seq[string]): string =
        # Generate SQL for this filter
```

---

## 6. Minor Issues

### 6.1 ✅ FIXED: Import Style
**Location**: Lines 1-6

```nim
import classes
import std/json
import std/oids
import std/strutils
import std/sequtils
import db_connector/db_sqlite
```

Should use consistent style:
```nim
import std/[json, oids, strutils, sequtils]
import db_connector/db_sqlite
import classes
```

### 6.2 Comment Style
Comments use `##` (documentation) even for private/internal code. Use `##` only for public API.

### 6.3 Variable Naming
- `pLimit`, `pOffset` are Hungarian notation style. Use `limitVal` or just `limit` with clear context.

---

## 7. Recommendations Summary

### Immediate (High Priority)
1. ~~**Replace `raiseAssert` with proper exceptions** for runtime errors~~ ✅ **RESOLVED**
2. ~~**Fix RootRef cast** - use proper typing~~ ✅ **RESOLVED**
3. **Optimize SQL string building** - use seq.join instead of repeated &
4. ~~**Fix implicit result shadowing** in `list()`~~ ✅ **RESOLVED**

### Short Term (Medium Priority)
1. **Add input validation** for field names to prevent SQL injection
2. **Remove unused types** (FilterOp, etc.)
3. ~~**Change methods to procs** where inheritance isn't needed~~ ⚠️ **BLOCKED** - `classes` library requires `method`
4. ~~**Document side effects** in upsert~~ ✅ **RESOLVED** (no more side effects)

### Long Term (Low Priority)
1. **Refactor NJSDBQuery** into smaller, focused types
2. **Implement prepared statement caching** for repeated queries
3. ~~**Add projection at SQL level** instead of post-processing~~ ✅ **RESOLVED**
4. **Add comprehensive API documentation** with examples

---

## 8. Recent Changes Summary

### API Naming Convention (Completed)
- `remove(id: string): int` → `removeOne(id: string): bool`
- `update(id: string, updates): int` → `updateOne(id: string, updates): bool`
- Query versions still return `int` (count)
- Removed `{.discardable.}` from `*One` variants

### Side Effects Fix (Completed)
- `writeDocument`, `upsert`, `put` no longer modify input documents
- Create copies using `document.copy()` before modification
- Fixed bug in `put` with missing `return` statement

### Projection Optimization (Completed)
- Added `buildProjectionSql()` helper
- Uses `json_object()` and `json_extract()` for flat field projections
- Falls back to in-memory projection for nested fields
- Both `list()` proc and iterator updated

### RootRef Removal (Completed)
- Changed `var db: RootRef` to `var db: pointer`
- `query()` method allocates stable heap copy using `alloc0()`
- Updated all casts to use `cast[ptr NJSDB](this.db)[]`

---

## 9. Positive Observations

1. ✅ Good use of SQLite JSON functions
2. ✅ Proper use of parameterized queries (SQL injection safe)
3. ✅ Clean chaining API design
4. ✅ Good separation of concerns between query building and execution
5. ✅ Proper transaction handling in `batch()`
6. ✅ Comprehensive test coverage (52 tests)
7. ✅ **Recent improvements show responsiveness to code review feedback**

---

## Overall Rating

| Category | Rating | Notes |
|----------|--------|-------|
| Idiomatic Nim | ⭐⭐⭐ | Good but has non-idiomatic patterns |
| Performance | ⭐⭐⭐ | SQL projection fixed, but string concat remains |
| Memory Safety | ⭐⭐⭐⭐ | RootRef fixed, some minor issues remain |
| API Clarity | ⭐⭐⭐⭐⭐ | Clean API, consistent naming convention |
| Code Structure | ⭐⭐⭐ | God objects and some duplication |

**Overall**: Good foundation with significant recent improvements. The main remaining concerns are performance (SQL string building) and code structure (god objects).
