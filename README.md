# n8n-nodes-better-mongodb

Two n8n nodes for MongoDB, built on the official `mongodb` driver. They reuse n8n's built-in **MongoDB** credential (`mongoDb`); there is no separate credential type. Self-hosted n8n only.

## Better MongoDB (action node)

| Resource | Operations |
|---|---|
| Document | find, findOne, insertOne/Many, updateOne/Many (operators or pipeline updates), replaceOne, deleteOne/Many, findOneAndUpdate/Replace/Delete, count, estimatedCount, distinct |
| Aggregation | aggregate (any stage is passed through: `$merge`, `$out`, `$lookup`, `$setWindowFields`, `$vectorSearch`, `$scoreFusion`, `$serializeEJSON`, `$function`, ...) |
| Bulk Write | bulkWrite (mixed ops, ordered/unordered), bulk upsert from input items by key fields |
| Collection | create (capped, time series, validators incl. 9.0 `constraint` level, clustered, pre/post images), create view, drop, rename, collMod, `$collStats` |
| Index | create (unique, TTL, partial, sparse, wildcard, hidden, collation), createMany, drop, dropAll, list, `$indexStats` |
| Search | Atlas Search `$search`, `$vectorSearch`, create/update/drop/list search indexes |
| GridFS | upload, download, list, delete, rename |
| Database & Admin | list databases/collections, drop database, runCommand, ping, buildInfo, database-level aggregate (`$currentOp`, ...), `$queryStats`, multi-collection client bulk write (server 8.0+) |

Extras: `explain`, collation, hint, `let`, `maxTimeMS`, read/write concern, read preference, and **Run in Transaction** (all items in one multi-document transaction, rolled back on any error).

### BSON types

- Input accepts Extended JSON anywhere: `{"$oid": "..."}`, `{"$date": "..."}`, `{"$numberDecimal": "..."}`, `{"$numberLong": "..."}`, `{"$binary": {...}}`, `{"$uuid": "..."}`, `{"$regularExpression": {...}}`, `$timestamp`, `$minKey`, `$maxKey`.
- *Convert ObjectId Fields* (default `_id`) turns 24-hex strings into ObjectId for the listed field names at any depth, including inside `$in`. *Convert Date Fields* does the same for ISO strings to Date.
- *Output Format*: **Simplified** (ObjectId, Date, Decimal128, Long as strings), **Extended JSON Relaxed**, or **Extended JSON Canonical** (lossless, type-preserving). Relaxed mode loses precision on integers beyond 2^53.

## Better MongoDB Trigger

Change stream trigger for a collection, a database or the entire cluster (replica set or sharded cluster required). Filter by event type, add `$match`/`$project` stages, choose full-document lookup, pre-images (`changeStreamPreAndPostImages`), and expanded DDL events. The resume token is stored in workflow static data, so events are not lost across restarts or n8n downtime (as long as the oplog still covers the gap).

## MongoDB 9.0 notes

Requires MongoDB 4.2+ (tested against 9.0) and Node.js 20.19+ (driver 7.9). On 9.0:

- Each operation is capped at the larger of 1 GB or 20% of RAM; heavier queries fail with error 146 or 292. Use *Allow Disk Use* or restructure the pipeline.
- `maxConcurrentMultiDocumentTransactions` defaults to 10000; *Run in Transaction* fails with `TooManyOpenTransactions` beyond that.
- `$queryStats` samples about 1% of operations by default, so results are a subset.
- `$function`, `$accumulator` and `$where` are no longer deprecated.
- Time series collections can be renamed.
- Per-operation `querySettings` is not exposed as an option (the driver does not forward it); send it with *Run Command*.
- Time series `timeField` must be a BSON Date: list it in *Convert Date Fields* or send `{"$date": "..."}`.

## Not included

Queryable Encryption and client-side field-level encryption need native `libmongocrypt` and are not supported. Use `runCommand` or the aggregation passthrough for anything else.

## Development

```
npm install
npm run lint
npm run build
npm run dev   # runs a local n8n with the nodes loaded
```
