## Multi-threading Example with Shared Connection (Thread Pool Pattern)
##
## This example demonstrates using NJSDB with a thread pool where each thread
## has its own connection but coordinates through a shared work queue.

import std/[os, strutils, json, locks, times, deques, random]
import njsdb

# Configuration
const
    NumWorkers = 4
    TotalTasks = 200
    DbFile = "multithread_shared.db"

# Thread-safe work queue
type
    WorkItem = object
        taskId: int
        operation: string  # "insert", "update", "query"
        data: JsonNode

    WorkQueue = object
        lock: Lock
        queue: Deque[WorkItem]
        finished: bool

# Global work queue
var gWorkQueue: WorkQueue

# Initialize work queue
proc initWorkQueue() =
    initLock(gWorkQueue.lock)
    gWorkQueue.queue = initDeque[WorkItem]()
    gWorkQueue.finished = false

# Add work item to queue
proc addWork(item: WorkItem) =
    withLock gWorkQueue.lock:
        gWorkQueue.queue.addLast(item)

# Get work item from queue (returns nil if finished)
proc getWork(): Option[WorkItem] =
    withLock gWorkQueue.lock:
        if gWorkQueue.queue.len > 0:
            return some(gWorkQueue.queue.popFirst())
        elif gWorkQueue.finished:
            return none(WorkItem)
    # Queue is empty but not finished, wait a bit
    sleep(10)
    return none(WorkItem)

# Mark queue as finished
proc finishWorkQueue() =
    withLock gWorkQueue.lock:
        gWorkQueue.finished = true

# Worker thread procedure
proc workerThread(workerId: int) {.thread.} =
    echo "Worker ", workerId, " started"

    # Each worker has its own database connection
    var db = NJSDB()
    db.open(DbFile)
    discard db.collection("tasks")

    var processed = 0

    while true:
        let workOpt = getWork()
        if workOpt.isNone:
            # Check if we should exit
            withLock gWorkQueue.lock:
                if gWorkQueue.finished and gWorkQueue.queue.len == 0:
                    break
            continue

        let work = workOpt.get()
        processed += 1

        case work.operation
        of "insert":
            # Insert document
            var doc = work.data
            doc["processedBy"] = %workerId
            doc["processedAt"] = %epochTime()
            db.put(doc)

        of "update":
            # Update document
            let taskId = work.data["taskId"].getStr
            db.updateOne(taskId, %*{
                "$set": {
                    "status": "completed",
                    "completedBy": workerId,
                    "completedAt": epochTime()
                }
            })

        of "query":
            # Query documents
            let status = work.data["status"].getStr
            let results = db.query()
                .where("status", "==", status)
                .limit(10)
                .list()
            # Just count results, don't print to avoid console spam
            discard results.len

        else:
            echo "Worker ", workerId, " unknown operation: ", work.operation

    echo "Worker ", workerId, " finished, processed ", processed, " tasks"
    db.close()

# Main procedure
proc main() =
    echo "=== NJSDB Thread Pool Example ==="
    echo "Workers: ", NumWorkers
    echo "Total tasks: ", TotalTasks
    echo ""

    # Clean up old database
    if fileExists(DbFile):
        removeFile(DbFile)

    # Initialize database with schema
    block:
        var db = NJSDB()
        db.open(DbFile)
        discard db.collection("tasks")
        db.close()

    # Initialize work queue
    initWorkQueue()

    # Create insert tasks
    echo "Creating insert tasks..."
    for i in 0..<TotalTasks:
        let task = WorkItem(
            taskId: i,
            operation: "insert",
            data: %*{
                "id": "task_" & $i,
                "type": "sample_task",
                "priority": rand(1..10),
                "status": "pending",
                "createdAt": epochTime()
            }
        )
        addWork(task)

    # Create some update tasks (will be processed after inserts)
    echo "Creating update tasks..."
    for i in 0..<TotalTasks div 2:
        let task = WorkItem(
            taskId: i,
            operation: "update",
            data: %*{ "taskId": "task_" & $i }
        )
        addWork(task)

    # Create some query tasks
    echo "Creating query tasks..."
    for i in 0..<20:
        let task = WorkItem(
            taskId: TotalTasks + i,
            operation: "query",
            data: %*{ "status": if i mod 2 == 0: "pending" else: "completed" }
        )
        addWork(task)

    # Mark queue as finished (no more tasks will be added)
    finishWorkQueue()

    echo ""
    echo "Starting workers..."
    let startTime = cpuTime()

    # Create and start worker threads
    var threads: array[NumWorkers, Thread[int]]
    for i in 0..<NumWorkers:
        createThread(threads[i], workerThread, i)

    # Wait for all workers to complete
    for i in 0..<NumWorkers:
        joinThread(threads[i])

    let totalTime = cpuTime() - startTime
    echo ""
    echo "=== All workers completed ==="
    echo "Total time: ", totalTime.formatFloat(ffDecimal, 3), "s"
    echo "Tasks per second: ", (TotalTasks + TotalTasks div 2 + 20).float / totalTime

    # Verification
    echo ""
    echo "=== Verification ==="
    var db = NJSDB()
    db.open(DbFile)
    discard db.collection("tasks")

    let totalDocs = db.query().count()
    let pending = db.query().where("status", "==", "pending").count()
    let completed = db.query().where("status", "==", "completed").count()

    echo "Total documents: ", totalDocs
    echo "Pending: ", pending
    echo "Completed: ", completed

    # Show sample completed task
    let sample = db.query()
        .where("status", "==", "completed")
        .limit(1)
        .get()

    if sample != nil:
        echo ""
        echo "Sample completed task:"
        echo "  ID: ", sample["id"].getStr
        echo "  Completed by worker: ", sample["completedBy"].getInt

    db.close()

    # Clean up
    if fileExists(DbFile):
        removeFile(DbFile)
        echo ""
        echo "Cleaned up database file"

    echo ""
    echo "=== Example completed ==="

# Run
randomize()
main()
