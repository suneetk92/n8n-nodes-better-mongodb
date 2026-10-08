# Changelog

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
