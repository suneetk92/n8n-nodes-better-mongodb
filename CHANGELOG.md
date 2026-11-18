# Changelog

## 1.2.0

- New option **On Invalid Date** (Options): *Skip Item* leaves out input items whose *Convert Date Fields* value is not a valid date and reports them in `skippedCount` / `skipped` (item index and reason) instead of failing the batch. Applies to insertOne, insertMany, bulkWrite and bulkUpsert with input-item sources. Default stays *Fail the Node*.

## 1.1.3

- Publish workflow now sets the package version from the git tag, so the npm version always matches the tag (1.1.1 and 1.1.2 failed to publish because `package.json` still said 1.1.0).
- Includes all changes from 1.1.1 and 1.1.2: clearer date conversion errors (item and value), the trigger's *Additional Pipeline* moved into *Options*, relaxed `n8n-workflow` peer dependency, updated dev dependencies.

## 1.1.0

- Updated the `mongodb` driver to 7.9.0 (fully compatible with MongoDB 9.0; requires Node.js 20.19+).
- Fixed *Convert Date Fields* silently skipping values: field names now match case-insensitively and ignore surrounding spaces, objects from other JS contexts are converted, and an unparseable date now raises an error naming the field instead of being sent to the server as a string (which time series collections reject).
- Documented MongoDB 9.0 behavior (memory limits, transaction cap, `$queryStats` sampling, `querySettings` via `runCommand`).

## 1.0.0

Initial release of **Better MongoDB** and **Better MongoDB Trigger**. Both reuse n8n's built-in MongoDB credential.

- Document CRUD, find-and-modify, count, distinct, `explain`, collation, hint, `let`, read/write concern.
- Aggregation with any stage passed through, plus database-level aggregation.
- Bulk write, bulk upsert from input items, and multi-collection client bulk write (MongoDB 8.0+).
- "Run in Transaction" option for multi-document transactions.
- Collection, view, index and Atlas Search / vector search index management, plus `$search` and `$vectorSearch`.
- GridFS upload, download, list, delete and rename.
- `runCommand`, `$currentOp`, `$queryStats`, and the MongoDB 9.0 `constraint` validation level.
- Extended JSON input, ObjectId and Date field conversion, and simplified / relaxed / canonical output.
- Change stream trigger for collection, database or cluster scope, with resume-token persistence.
