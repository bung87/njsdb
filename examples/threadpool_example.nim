## ThreadPoolNJSDB Example
##
## This example demonstrates the user-friendly ThreadPoolNJSDB wrapper
## for multi-threaded NJSDB operations.

import std/[os, json, times, strutils]
import threadpool_njsdb

const
    NumWorkers = 4
    TotalDocs = 200
    DbFile = "threadpool_example.db"

proc main() =
    echo "=== ThreadPoolNJSDB Example ==="
    echo "Workers: ", NumWorkers
    echo "Total documents: ", TotalDocs
    echo ""

    # Clean up old database
    if fileExists(DbFile):
        removeFile(DbFile)
        echo "Removed old database file"

    # Create and start thread pool
    echo "Creating thread pool..."
    let pool = newThreadPoolNJSDB(DbFile, numWorkers = NumWorkers)
    pool.start()
    echo "Thread pool started"
    echo ""

    # Example 1: Async insert
    echo "=== Example 1: Async Insert ==="
    let startTime = cpuTime()
    
    for i in 0..<TotalDocs:
        let doc = %*{
            "id": "doc_" & $i,
            "sequence": i,
            "value": i * 10,
            "category": i mod 4,
            "timestamp": epochTime()
        }
        pool.insert("documents", doc)
    
    # Wait for all inserts to complete
    pool.waitForAll()
    
    let insertTime = cpuTime() - startTime
    echo "Inserted ", TotalDocs, " documents in ", insertTime.formatFloat(ffDefault, 3), "s"
    echo "Rate: ", (TotalDocs.float / insertTime).formatFloat(ffDefault, 1), " docs/sec"
    echo ""

    # Example 2: Batch insert
    echo "=== Example 2: Batch Insert ==="
    var batchDocs: seq[JsonNode]
    for i in 0..<50:
        batchDocs.add(%*{
            "id": "batch_" & $i,
            "batchId": i,
            "type": "batch_item"
        })
    
    let batchStart = cpuTime()
    let submitted = pool.insertBatch("documents", batchDocs)
    pool.waitForAll()
    let batchTime = cpuTime() - batchStart
    echo "Batch submitted ", submitted, " documents in ", batchTime.formatFloat(ffDefault, 3), "s"
    echo ""

    # Example 3: Update operation
    echo "=== Example 3: Update Operation ==="
    let updateStart = cpuTime()
    pool.update("documents", 
        "category",
        1.0,
        %*{ "$set": { "updated": 1, "updateTime": epochTime() } }
    )
    pool.waitForAll()
    let updateTime = cpuTime() - updateStart
    echo "Update operation completed in ", updateTime.formatFloat(ffDefault, 3), "s"
    echo ""

    # Example 4: Delete operation
    echo "=== Example 4: Delete Operation ==="
    pool.delete("documents", "doc_0")
    pool.delete("documents", "doc_1")
    pool.waitForAll()
    echo "Delete operations submitted"
    echo ""

    # Stop thread pool
    echo "Stopping thread pool..."
    pool.stop()
    echo "Thread pool stopped"

    # Clean up
    if fileExists(DbFile):
        removeFile(DbFile)
        echo "Cleaned up database file"

    echo ""
    echo "=== Example completed successfully ==="

main()
