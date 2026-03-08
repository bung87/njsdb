## Multi-threading Example for NJSDB
##
## This example demonstrates how to use NJSDB in a multi-threaded environment.
## Each thread has its own database connection, but they share the same database file.
##
## Note: SQLite uses database-level locking. When multiple threads write concurrently,
## some threads may get "database is locked" errors. This example shows how to handle
## that with retry logic.

import std/[os, strutils, json, times, random]
import njsdb
import db_connector/db_sqlite

# Configuration
const
    NumThreads = 4
    DocumentsPerThread = 50
    DbFile = "multithread_test.db"
    MaxRetries = 10
    BaseDelayMs = 10

# Thread data structure
type
    ThreadData = object
        threadId: int

# Initialize database schema (called once before threads start)
proc initDatabase(filename: string) =
    var db = NJSDB()
    db.open(filename)
    discard db.collection("documents")
    db.close()

# Worker thread procedure
proc workerThread(data: ThreadData) {.thread.} =
    let threadId = data.threadId
    echo "Thread ", threadId, " started"

    # Each thread creates its own database connection
    var db = NJSDB()
    db.open(DbFile)
    discard db.collection("documents")

    # Insert documents with retry logic
    let startTime = cpuTime()
    var inserted = 0
    for i in 0..<DocumentsPerThread:
        let docId = "thread" & $threadId & "_doc" & $i
        let doc = %*{
            "id": docId,
            "threadId": threadId,
            "sequence": i,
            "value": i * 10,
            "timestamp": epochTime(),
            "data": "Data from thread " & $threadId & " document " & $i
        }
        
        # Retry insert with exponential backoff
        var retries = 0
        var success = false
        while retries < MaxRetries and not success:
            try:
                db.put(doc)
                success = true
                inserted += 1
            except DbError:
                retries += 1
                if retries < MaxRetries:
                    let delay = BaseDelayMs * (1 shl retries)
                    sleep(delay)
        
        if not success:
            echo "Thread ", threadId, " failed to insert doc ", i, " after ", MaxRetries, " retries"

    let insertTime = cpuTime() - startTime
    echo "Thread ", threadId, " inserted ", inserted, "/", DocumentsPerThread, " documents in ", insertTime.formatFloat(ffDecimal, 3), "s"

    # Query documents inserted by this thread (simple query without sorting)
    let queryStart = cpuTime()
    let results = db.query()
        .where("threadId", "==", threadId.float)
        .list()
    let queryTime = cpuTime() - queryStart

    echo "Thread ", threadId, " queried ", results.len, " documents in ", queryTime.formatFloat(ffDecimal, 3), "s"

    # Update some documents with retry logic
    let updateStart = cpuTime()
    var updateRetries = 0
    var updateSuccess = false
    while updateRetries < MaxRetries and not updateSuccess:
        try:
            db.query()
                .where("threadId", "==", threadId.float)
                .where("sequence", "<", 25)
                .update(%*{
                    "$set": { "updated": 1, "updatedBy": threadId }
                })
            updateSuccess = true
        except DbError:
            updateRetries += 1
            if updateRetries < MaxRetries:
                let delay = BaseDelayMs * (1 shl updateRetries)
                sleep(delay)
    
    if not updateSuccess:
        echo "Thread ", threadId, " failed to update documents"
    
    let updateTime = cpuTime() - updateStart
    echo "Thread ", threadId, " updated documents in ", updateTime.formatFloat(ffDecimal, 3), "s"

    # Close database connection
    db.close()
    echo "Thread ", threadId, " finished"

# Main procedure
proc main() =
    echo "=== NJSDB Multi-threading Example ==="
    echo "Threads: ", NumThreads
    echo "Documents per thread: ", DocumentsPerThread
    echo ""

    # Clean up old database file
    if fileExists(DbFile):
        removeFile(DbFile)
        echo "Removed old database file: ", DbFile

    # Initialize database
    initDatabase(DbFile)
    echo "Database initialized"
    echo ""

    # Create and start threads
    var threads: array[NumThreads, Thread[ThreadData]]
    var threadData: array[NumThreads, ThreadData]

    let totalStart = cpuTime()

    for i in 0..<NumThreads:
        threadData[i] = ThreadData(threadId: i)
        createThread(threads[i], workerThread, threadData[i])

    # Wait for all threads to complete
    for i in 0..<NumThreads:
        joinThread(threads[i])

    let totalTime = cpuTime() - totalStart
    echo ""
    echo "=== All threads completed ==="
    echo "Total time: ", totalTime.formatFloat(ffDecimal, 3), "s"

    # Verify results in main thread
    echo ""
    echo "=== Verification ==="
    var db = NJSDB()
    db.open(DbFile)
    discard db.collection("documents")

    let totalDocs = db.query().count()
    echo "Total documents in database: ", totalDocs

    let expectedDocs = NumThreads * DocumentsPerThread
    if totalDocs == expectedDocs:
        echo "✓ Document count matches expected: ", expectedDocs
    else:
        echo "✗ Document count mismatch! Expected: ", expectedDocs, ", Got: ", totalDocs

    # Count updated documents
    let updatedDocs = db.query()
        .where("updated", "==", 1)
        .count()
    echo "Updated documents: ", updatedDocs

    # Sample query from each thread
    echo ""
    echo "=== Sample data from each thread ==="
    for i in 0..<NumThreads:
        let sample = db.query()
            .where("threadId", "==", i.float)
            .limit(2)
            .list()
        echo "Thread ", i, " samples:"
        for doc in sample:
            echo "  - ", doc["id"].getStr, " (seq=", doc["sequence"].getInt, ")"

    db.close()

    # Clean up
    if fileExists(DbFile):
        removeFile(DbFile)
        echo ""
        echo "Cleaned up database file: ", DbFile

    echo ""
    echo "=== Example completed successfully ==="

# Run main
randomize()
main()
