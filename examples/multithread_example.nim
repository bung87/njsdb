## Multi-threading Example for NJSDB
##
## This example demonstrates how to use NJSDB in a multi-threaded environment.
## Each thread has its own database connection, but they share the same database file.

import std/[os, strutils, json, locks, times]
import njsdb

# Configuration
const
    NumThreads = 4
    DocumentsPerThread = 100
    DbFile = "multithread_test.db"

# Thread data structure
type
    ThreadData = object
        threadId: int
        db: NJSDB

# Initialize database schema (called once before threads start)
proc initDatabase(filename: string) =
    var db = NJSDB()
    db.open(filename)
    db.collection("documents")
    db.close()

# Worker thread procedure
proc workerThread(data: ThreadData) {.thread.} =
    let threadId = data.threadId
    echo "Thread ", threadId, " started"

    # Each thread creates its own database connection
    var db = NJSDB()
    db.open(DbFile)
    db.collection("documents")

    # Insert documents
    let startTime = cpuTime()
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
        db.put(doc)

    let insertTime = cpuTime() - startTime
    echo "Thread ", threadId, " inserted ", DocumentsPerThread, " documents in ", insertTime.formatFloat(ffDecimal, 3), "s"

    # Query documents inserted by this thread
    let queryStart = cpuTime()
    let results = db.query()
        .where("threadId", "==", threadId)
        .sort("sequence", ascending = true, isNumber = true)
        .list()
    let queryTime = cpuTime() - queryStart

    echo "Thread ", threadId, " queried ", results.len, " documents in ", queryTime.formatFloat(ffDecimal, 3), "s"

    # Update some documents
    let updateStart = cpuTime()
    db.query()
        .where("threadId", "==", threadId)
        .where("sequence", "<", 50)
        .update(%*{
            "$set": { "updated": true, "updatedBy": threadId }
        })
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
        threadData[i] = ThreadData(threadId: i, db: NJSDB())
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
    db.collection("documents")

    let totalDocs = db.query().count()
    echo "Total documents in database: ", totalDocs

    let expectedDocs = NumThreads * DocumentsPerThread
    if totalDocs == expectedDocs:
        echo "✓ Document count matches expected: ", expectedDocs
    else:
        echo "✗ Document count mismatch! Expected: ", expectedDocs, ", Got: ", totalDocs

    # Count updated documents
    let updatedDocs = db.query()
        .where("updated", "==", true)
        .count()
    echo "Updated documents: ", updatedDocs

    # Sample query from each thread
    echo ""
    echo "=== Sample data from each thread ==="
    for i in 0..<NumThreads:
        let sample = db.query()
            .where("threadId", "==", i)
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
main()
