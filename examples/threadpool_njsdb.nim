## ThreadPoolNJSDB - User-friendly multi-threading wrapper for NJSDB
##
## This module provides a simple API for using NJSDB in multi-threaded applications.
## It handles connection pooling, retry logic, and task distribution automatically.

import std/[os, json, times, random, locks, deques]
import njsdb
import db_connector/db_sqlite

export njsdb, json

type
    TaskType = enum
        ttInsert, ttUpdate, ttQuery, ttDelete

    Task = object
        id: int
        taskType: TaskType
        collection: string
        doc: JsonNode
        docId: string
        queryFilter: JsonNode
        updateOps: JsonNode
        fieldName: string
        fieldValue: float

    ThreadPoolNJSDB* = ref object
        dbPath: string
        numWorkers: int
        workers: seq[Worker]
        taskQueue: Deque[Task]
        queueLock: Lock
        queueCond: Cond
        shutdown: bool
        nextTaskId: int
        taskIdLock: Lock

    Worker = object
        id: int
        thread: Thread[ptr Worker]
        pool: ThreadPoolNJSDB
        db: NJSDB

const
    DefaultMaxRetries = 10
    DefaultBaseDelayMs = 10

# Forward declarations
proc workerLoop(worker: ptr Worker) {.thread.}

# Create a new thread pool
proc newThreadPoolNJSDB*(dbPath: string, numWorkers: int = 4): ThreadPoolNJSDB =
    ## Create a new thread pool for NJSDB operations
    result = ThreadPoolNJSDB(
        dbPath: dbPath,
        numWorkers: numWorkers,
        workers: newSeq[Worker](numWorkers),
        taskQueue: initDeque[Task](),
        shutdown: false,
        nextTaskId: 0
    )
    initLock(result.queueLock)
    initCond(result.queueCond)
    initLock(result.taskIdLock)

# Start the thread pool
proc start*(pool: ThreadPoolNJSDB) =
    ## Start all worker threads
    if not fileExists(pool.dbPath):
        var db = NJSDB()
        db.open(pool.dbPath)
        db.close()
    
    for i in 0..<pool.numWorkers:
        pool.workers[i].id = i
        pool.workers[i].pool = pool
        pool.workers[i].db = NJSDB()
        pool.workers[i].db.open(pool.dbPath)
        createThread(pool.workers[i].thread, workerLoop, addr pool.workers[i])

# Stop the thread pool
proc stop*(pool: ThreadPoolNJSDB) =
    ## Stop all worker threads gracefully
    withLock pool.queueLock:
        pool.shutdown = true
    signal(pool.queueCond)
    
    for i in 0..<pool.numWorkers:
        joinThread(pool.workers[i].thread)
        pool.workers[i].db.close()

# Generate next task ID
proc nextId(pool: ThreadPoolNJSDB): int =
    withLock pool.taskIdLock:
        result = pool.nextTaskId
        pool.nextTaskId += 1

# Submit a task to the pool
proc submitTask(pool: ThreadPoolNJSDB, task: Task) =
    withLock pool.queueLock:
        pool.taskQueue.addLast(task)
    signal(pool.queueCond)

# Insert a document (async)
proc insert*(pool: ThreadPoolNJSDB, collection: string, doc: JsonNode) =
    ## Insert a document asynchronously
    var task = Task(
        id: pool.nextId(),
        taskType: ttInsert,
        collection: collection,
        doc: doc
    )
    pool.submitTask(task)

# Update documents matching filter
proc update*(pool: ThreadPoolNJSDB, collection: string, fieldName: string, fieldValue: float, updateOps: JsonNode) =
    ## Update documents asynchronously where field == value
    var task = Task(
        id: pool.nextId(),
        taskType: ttUpdate,
        collection: collection,
        fieldName: fieldName,
        fieldValue: fieldValue,
        updateOps: updateOps
    )
    pool.submitTask(task)

# Delete document by ID
proc delete*(pool: ThreadPoolNJSDB, collection: string, docId: string) =
    ## Delete a document asynchronously by ID
    var task = Task(
        id: pool.nextId(),
        taskType: ttDelete,
        collection: collection,
        docId: docId
    )
    pool.submitTask(task)

# Wait for all pending tasks to complete
proc waitForAll*(pool: ThreadPoolNJSDB) =
    ## Block until all pending tasks are completed
    while true:
        withLock pool.queueLock:
            if pool.taskQueue.len == 0:
                break
        sleep(10)
    sleep(100)

# Worker thread main loop
proc workerLoop(worker: ptr Worker) {.thread.} =
    let pool = worker.pool
    
    while true:
        var task: Task
        var hasTask = false
        
        withLock pool.queueLock:
            if pool.taskQueue.len > 0:
                task = pool.taskQueue.popFirst()
                hasTask = true
            elif pool.shutdown:
                break
        
        if not hasTask:
            withLock pool.queueLock:
                if pool.taskQueue.len == 0 and not pool.shutdown:
                    wait(pool.queueCond, pool.queueLock)
            continue
        
        # Execute task with retry logic
        # First, ensure collection is selected with retry
        var colRetries = 0
        var colSuccess = false
        while colRetries < DefaultMaxRetries and not colSuccess:
            try:
                discard worker.db.collection(task.collection)
                colSuccess = true
            except DbError:
                colRetries += 1
                if colRetries < DefaultMaxRetries:
                    sleep(DefaultBaseDelayMs * (1 shl colRetries))
        
        if not colSuccess:
            continue
        
        case task.taskType
        of ttInsert:
            var retries = 0
            var success = false
            while retries < DefaultMaxRetries and not success:
                try:
                    worker.db.put(task.doc)
                    success = true
                except DbError:
                    retries += 1
                    if retries < DefaultMaxRetries:
                        sleep(DefaultBaseDelayMs * (1 shl retries))
        
        of ttUpdate:
            var retries = 0
            var success = false
            while retries < DefaultMaxRetries and not success:
                try:
                    worker.db.query()
                        .where(task.fieldName, "==", task.fieldValue)
                        .update(task.updateOps)
                    success = true
                except DbError:
                    retries += 1
                    if retries < DefaultMaxRetries:
                        sleep(DefaultBaseDelayMs * (1 shl retries))
        
        of ttDelete:
            var retries = 0
            var success = false
            while retries < DefaultMaxRetries and not success:
                try:
                    if task.docId.len > 0:
                        discard worker.db.delete(task.docId)
                    success = true
                except DbError:
                    retries += 1
                    if retries < DefaultMaxRetries:
                        sleep(DefaultBaseDelayMs * (1 shl retries))
        
        of ttQuery:
            discard

# Convenience proc for batch insert
proc insertBatch*(pool: ThreadPoolNJSDB, collection: string, docs: seq[JsonNode]): int =
    ## Insert multiple documents, returns count of submitted tasks
    for doc in docs:
        pool.insert(collection, doc)
    return docs.len
