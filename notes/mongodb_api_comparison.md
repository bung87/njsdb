# NJSDB vs MongoDB API 对比

## 基础操作

| NJSDB | MongoDB | 说明 |
|----------|---------|------|
| `open(path)` | `MongoClient.connect()` | 连接数据库 |
| `collection(name)` | `db.collection(name)` | 选择集合 |
| `close()` | `client.close()` | 关闭连接 |

## 文档操作

| NJSDB | MongoDB | 说明 |
|----------|---------|------|
| `put(doc)` | `insertOne(doc)` | 插入文档 |
| `put(doc, merge=true)` | `updateOne({_id: id}, {$set: doc}, {upsert: true})` | 合并更新 |
| `get(id)` | `findOne({_id: id})` | 根据ID获取 |
| `delete(id)` | `deleteOne({_id: id})` | 删除单个 |
| `upsert(doc)` | `replaceOne({_id: id}, doc, {upsert: true})` | 存在更新，不存在插入 |

## 查询操作

| NJSDB | MongoDB | 说明 |
|----------|---------|------|
| `query().list()` | `find(filter).toArray()` | 查询列表 |
| `query().get()` | `findOne(filter)` | 查询单个 |
| `query().count()` | `countDocuments(filter)` | 计数 |
| `query().where(field, op, value)` | `find({field: {$op: value}})` | 条件查询 |
| `query().filter(json)` | `find(filter)` | JSON条件查询 |
| `query().sort(field, asc)` | `find().sort({field: 1})` | 排序 |
| `query().limit(n)` | `find().limit(n)` | 限制数量 |
| `query().skip(n)` | `find().skip(n)` | 跳过文档 |
| `query().distinctValues(field)` | `distinct(field)` | 去重值 |

## 批量操作

| NJSDB | MongoDB | 说明 |
|----------|---------|------|
| `insertMany(docs)` | `insertMany(docs)` | 批量插入 |
| `query().delete()` | `deleteMany(filter)` | 条件删除多个 |
| `query().update(updates)` | `updateMany(filter, updates)` | 条件更新多个 |
| `batch(proc)` | `withTransaction()` | 事务批处理 |

## 聚合操作

| NJSDB | MongoDB | 说明 |
|----------|---------|------|
| `aggregate(pipeline)` | `aggregate(pipeline)` | 聚合管道 |
| `aggregate(groupField, ops)` | `aggregate([{$group}])` | 简化聚合 |

### 聚合示例

```nim
# 分组计数（替代 aggregateCount）
db.collection("orders").aggregate(@[
  %*{ "$group": {
    "_id": "$status",
    "count": { "$sum": 1 }
  }}
])

# 带过滤的分组计数
db.collection("orders").aggregate(@[
  %*{ "$match": { "amount": { "$gt": 100 } } },
  %*{ "$group": {
    "_id": "$status",
    "count": { "$sum": 1 },
    "total": { "$sum": "$amount" }
  }}
])
```

### 支持的聚合阶段

| 阶段 | NJSDB | MongoDB |
|------|----------|---------|
| `$match` | ✅ | ✅ |
| `$group` | ✅ | ✅ |
| `$sort` | ✅ | ✅ |
| `$limit` | ✅ | ✅ |
| `$skip` | ✅ | ✅ |
| `$project` | ✅ | ✅ |
| `$count` | ✅ | ✅ |
| `$lookup` | ❌ | ✅ |
| `$unwind` | ❌ | ✅ |
| `$facet` | ❌ | ✅ |

### 支持的聚合操作符

| 操作符 | NJSDB | MongoDB |
|--------|----------|---------|
| `$sum` | ✅ | ✅ |
| `$avg` | ✅ | ✅ |
| `$min` | ✅ | ✅ |
| `$max` | ✅ | ✅ |
| `$first` | ❌ | ✅ |
| `$last` | ❌ | ✅ |
| `$push` | ❌ | ✅ |
| `$addToSet` | ❌ | ✅ |

## 查询操作符

| 操作符 | NJSDB | MongoDB |
|--------|----------|---------|
| `$eq` | ✅ | ✅ |
| `$ne` | ✅ | ✅ |
| `$gt` | ✅ | ✅ |
| `$gte` | ✅ | ✅ |
| `$lt` | ✅ | ✅ |
| `$lte` | ✅ | ✅ |
| `$in` | ✅ | ✅ |
| `$nin` | ✅ | ✅ |
| `$exists` | ✅ | ✅ |
| `$type` | ✅ | ✅ |
| `$regex` | ❌ | ✅ |
| `$all` | ✅ | ✅ |
| `$size` | ✅ | ✅ |
| `$or` | ✅ | ✅ |
| `$and` | ✅ | ✅ |
| `$not` | ✅ | ✅ |
| `$nor` | ✅ | ✅ |

## 更新操作符

| 操作符 | NJSDB | MongoDB |
|--------|----------|---------|
| `$set` | ✅ | ✅ |
| `$unset` | ✅ | ✅ |
| `$inc` | ✅ | ✅ |
| `$mul` | ✅ | ✅ |
| `$rename` | ✅ | ✅ |
| `$push` | ❌ | ✅ |
| `$pull` | ❌ | ✅ |
| `$addToSet` | ❌ | ✅ |
| `$pop` | ❌ | ✅ |
| `$slice` | ❌ | ✅ |

## 主要差异总结

### NJSDB 特有
- `put()` 同时支持 insert 和 replace（单一方法处理插入和替换）
- `query()` 返回查询构建器，支持链式调用（流畅接口）
- `batch()` 事务批处理（替代 MongoDB 的 withTransaction）

### MongoDB 特有
- 数组操作符（$push, $pull, $addToSet, $pop）
- $lookup 关联查询
- $unwind 展开数组
- 地理空间查询
- 文本搜索
- 更丰富的聚合操作符
