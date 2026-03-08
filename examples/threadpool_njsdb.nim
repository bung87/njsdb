## ThreadPoolNJSDB - User-friendly multi-threading wrapper for NJSDB
##
## This module provides a simple API for using NJSDB in multi-threaded applications.
## It handles connection pooling, retry logic, and task distribution automatically.
##
## NOTE: This implementation uses a simple lock-based task queue.
## Each worker thread has its own isolated database connection.

import std/[os, json, locks]
import njsdb
import db_connector/db_sqlite

export njsdb, json

# Task types
type
    TaskType = enum
        ttInsert, ttUpdate, ttDelete, ttShutdown

    Task = object
        taskType: TaskType
        collection: string
        doc: JsonNode
        docId: string
        fieldName: string
        fieldValue: float
        updateOps: JsonNode

    TaskQueue = object
        tasks: seq[Task]
        lock: Lock
        cond: Cond
        shutdown: bool

    ThreadPoolNJSDB* = object
        ## Thread-safe thread pool for NJSDB operations.
        ## This object should be created in the main thread only.
        dbPath: string
        numWorkers: int
        workers: seq[Thread[ptr TaskQueue]]
        queue: TaskQueue
        started: bool

const
    DefaultMaxRetries = 10
    DefaultBaseDelayMs = 10

# Initialize task queue
proc initQueue(q: var TaskQueue) =
    q.tasks = @[]
    q.shutdown = false
    initLock(q.lock)
    initCond(q.cond)

# Add task to queue
proc enqueue(q: var TaskQueue, task: Task) =
    withLock q.lock:
        q.tasks.add(task)
    signal(q.cond)

# Get task from queue (blocking)
proc dequeue(q: var TaskQueue): Task =
    withLock q.lock:
        while q.tasks.len == 0 and not q.shutdown:
            wait(q.cond, q.lock)
        
        if q.shutdown:
            return Task(taskType: ttShutdown)
        
        result = q.tasks[0]
        q.tasks.delete(0)

# Mark queue as shutdown
proc shutdown(q: var TaskQueue) =
    withLock q.lock:
        q.shutdown = true
    broadcast(q.cond)

# Get queue size
proc len(q: var TaskQueue): int =
    withLock q.lock:
        return q.tasks.len

# Worker thread procedure - each worker has its own database connection
proc workerProc(queue: ptr TaskQueue) {.thread.} =
    ## Worker thread - each has its own isolated database connection
    
    # Get the database path from first task
    var dbPath = ""
    var db: NJSDB
    var dbOpened = false
    
    var running = true
    while running:
        let task = queue[].dequeue()
        
        case task.taskType
        of ttShutdown:
            running = false
            
        of ttInsert:
            if not dbOpened:
                # First task - open database
                db = NJSDB()
                db.open(task.doc["__dbPath"].getStr)
                dbOpened = true
            
            # Retry collection selection with exponential backoff
            var colRetries = 0
            var colSuccess = false
            while colRetries < DefaultMaxRetries and not colSuccess:
                try:
                    discard db.collection(task.collection)
                    colSuccess = true
                except DbError:
                    colRetries += 1
                    if colRetries < DefaultMaxRetries:
                        sleep(DefaultBaseDelayMs * (1 shl colRetries))
            
            if not colSuccess:
                continue
            
            var retries = 0
            var success = false
            while retries < DefaultMaxRetries and not success:
                try:
                    # Remove internal field before storing
                    var cleanDoc = task.doc
                    if cleanDoc.hasKey("__dbPath"):
                        cleanDoc.delete("__dbPath")
                    db.put(cleanDoc)
                    success = true
                except DbError:
                    retries += 1
                    if retries < DefaultMaxRetries:
                        sleep(DefaultBaseDelayMs * (1 shl retries))
        
        of ttUpdate:
            if not dbOpened:
                db = NJSDB()
                db.open(task.collection)  # Use collection as temp
                dbOpened = true
            
            discard db.collection(task.collection)
            var retries = 0
            var success = false
            while retries < DefaultMaxRetries and not success:
                try:
                    db.query()
                        .where(task.fieldName, "==", task.fieldValue)
                        .update(task.updateOps)
                    success = true
                except DbError:
                    retries += 1
                    if retries < DefaultMaxRetries:
                        sleep(DefaultBaseDelayMs * (1 shl retries))
        
        of ttDelete:
            if not dbOpened:
                db = NJSDB()
                db.open(task.collection)
                dbOpened = true
            
            discard db.collection(task.collection)
            var retries = 0
            var success = false
            while retries < DefaultMaxRetries and not success:
                try:
                    if task.docId.len > 0:
                        discard db.delete(task.docId)
                    success = true
                except DbError:
                    retries += 1
                    if retries < DefaultMaxRetries:
                        sleep(DefaultBaseDelayMs * (1 shl retries))
    
    if dbOpened:
        db.close()

# Create a new thread pool
proc newThreadPoolNJSDB*(dbPath: string, numWorkers: int = 4): ThreadPoolNJSDB =
    ## Create a new thread pool for NJSDB operations.
    ##
    ## Example:
    ##   var pool = newThreadPoolNJSDB("mydb.db", numWorkers = 4)
    ##   pool.start()
    ##   # ... use pool ...
    ##   pool.stop()
    
    result = ThreadPoolNJSDB(
        dbPath: dbPath,
        numWorkers: numWorkers,
        workers: newSeq[Thread[ptr TaskQueue]](numWorkers),
        started: false
    )
    initQueue(result.queue)

# Start the thread pool
proc start*(pool: var ThreadPoolNJSDB) =
    ## Start all worker threads.
    ## Must be called before submitting tasks.
    
    if pool.started:
        return
    
    # Ensure database file exists
    if not fileExists(pool.dbPath):
        var db = NJSDB()
        db.open(pool.dbPath)
        db.close()
    
    # Start workers
    for i in 0..<pool.numWorkers:
        createThread(pool.workers[i], workerProc, addr pool.queue)
    
    pool.started = true

# Stop the thread pool
proc stop*(pool: var ThreadPoolNJSDB) =
    ## Stop all worker threads gracefully.
    ## Sends shutdown signal to all workers and waits for completion.
    
    if not pool.started:
        return
    
    # Send shutdown signal to all workers
    pool.queue.shutdown()
    
    # Wait for all workers to finish
    for i in 0..<pool.numWorkers:
        joinThread(pool.workers[i])
    
    pool.started = false

# Submit a task to the pool
proc submitTask(pool: var ThreadPoolNJSDB, task: Task) =
    ## Submit a task to the worker pool.
    pool.queue.enqueue(task)

# Insert a document (async)
proc insert*(pool: var ThreadPoolNJSDB, collection: string, doc: JsonNode) =
    ## Insert a document asynchronously.
    ## The document will be processed by an available worker.
    ##
    ## Example:
    ##   pool.insert("users", %*{ "id": "1", "name": "Alice" })
    
    # Add dbPath to doc for worker to use
    var docWithPath = doc
    docWithPath["__dbPath"] = %pool.dbPath
    
    let task = Task(
        taskType: ttInsert,
        collection: collection,
        doc: docWithPath
    )
    pool.submitTask(task)

# Update documents matching filter
proc update*(pool: var ThreadPoolNJSDB, collection: string, fieldName: string, fieldValue: float, updateOps: JsonNode) =
    ## Update documents asynchronously where field == value.
    ##
    ## Example:
    ##   pool.update("users", "status", 0.0, %*{ "$set": { "status": 1 } })
    
    let task = Task(
        taskType: ttUpdate,
        collection: collection,
        fieldName: fieldName,
        fieldValue: fieldValue,
        updateOps: updateOps
    )
    pool.submitTask(task)

# Delete document by ID
proc delete*(pool: var ThreadPoolNJSDB, collection: string, docId: string) =
    ## Delete a document asynchronously by ID.
    ##
    ## Example:
    ##   pool.delete("users", "user_123")
    
    let task = Task(
        taskType: ttDelete,
        collection: collection,
        docId: docId
    )
    pool.submitTask(task)

# Wait for all pending tasks to complete
proc waitForAll*(pool: var ThreadPoolNJSDB) =
    ## Block until all pending tasks are completed.
    ## This is a best-effort wait - it sleeps until the queue is empty.
    ##
    ## Note: There may still be tasks being processed by workers when this returns.
    
    while pool.queue.len() > 0:
        sleep(10)
    
    # Give workers time to finish current task
    sleep(100)

# Convenience proc for batch insert
proc insertBatch*(pool: var ThreadPoolNJSDB, collection: string, docs: seq[JsonNode]): int =
    ## Insert multiple documents, returns count of submitted tasks.
    ##
    ## Example:
    ##   let docs = @[%*{ "id": "1" }, %*{ "id": "2" }]
    ##   let count = pool.insertBatch("users", docs)
    
    for doc in docs:
        pool.insert(collection, doc)
    return docs.len

# Check if pool is started
proc isStarted*(pool: ThreadPoolNJSDB): bool =
    ## Check if the thread pool has been started.
    return pool.started

# Get number of pending tasks
proc pendingTasks*(pool: var ThreadPoolNJSDB): int =
    ## Get the number of tasks waiting in the queue.
    return pool.queue.len()
