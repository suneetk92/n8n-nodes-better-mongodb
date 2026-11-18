/* eslint-disable @typescript-eslint/no-explicit-any */
import { GridFSBucket, ObjectId } from 'mongodb';
import type { ClientSession, Db, MongoClient } from 'mongodb';
import { NodeOperationError } from 'n8n-workflow';
import type { IBinaryKeyData, IDataObject, IExecuteFunctions, INodeExecutionData } from 'n8n-workflow';
import { asObject, convertFields, parseEjson, toFieldSet } from './GenericFunctions';
import type { ConvertOptions } from './GenericFunctions';

export interface Ctx {
	fn: IExecuteFunctions;
	client: MongoClient;
	db: Db;
	session?: ClientSession;
	convert: ConvertOptions;
	options: IDataObject;
	items: INodeExecutionData[];
	i: number;
}

export interface Out {
	json: any;
	binary?: IBinaryKeyData;
}

type Handler = (c: Ctx) => Promise<Out[]>;

const HEX24 = /^[0-9a-fA-F]{24}$/;
const wrap = (rows: any[]): Out[] => rows.map((json) => ({ json: asObject(json) }));
const one = (row: any): Out[] => wrap([row]);

const p = (c: Ctx, name: string, def: any = ''): any => c.fn.getNodeParameter(name, c.i, def);

function pj(c: Ctx, name: string, raw?: unknown): any {
	const value = parseEjson(c.fn.getNode(), raw ?? p(c, name, ''), name, c.i);
	return value === undefined ? undefined : convertFields(value, c.convert);
}

/** Converts an input item, reporting which item / value failed (errors would otherwise lose their context). */
function conv(c: Ctx, idx: number): any {
	try {
		return convertFields(c.items[idx].json, c.convert);
	} catch (error) {
		throw new NodeOperationError(c.fn.getNode(), (error as Error).message, { itemIndex: idx });
	}
}

function coll(c: Ctx) {
	const o = c.options;
	const opts: any = {};
	if (!c.session) {
		if (o.readPreference) opts.readPreference = o.readPreference;
		if (o.readConcern) opts.readConcern = { level: o.readConcern };
		if (o.writeConcern) {
			const w = String(o.writeConcern);
			opts.writeConcern = { w: /^\d+$/.test(w) ? Number(w) : w };
		}
	}
	return c.db.collection(p(c, 'collection'), opts);
}

const sess = (c: Ctx): any => (c.session ? { session: c.session } : {});

function queryOptions(c: Ctx): { opts: any; explain: string } {
	const q = p(c, 'queryOptions', {}) as IDataObject;
	const opts: any = { ...sess(c) };
	if (q.collation) opts.collation = parseEjson(c.fn.getNode(), q.collation, 'collation', c.i);
	if (q.hint) {
		const h = String(q.hint).trim();
		opts.hint = h.startsWith('{') ? parseEjson(c.fn.getNode(), h, 'hint', c.i) : h;
	}
	if (q.maxTimeMS) opts.maxTimeMS = q.maxTimeMS;
	if (q.comment) opts.comment = q.comment;
	if (q.batchSize) opts.batchSize = q.batchSize;
	if (q.allowDiskUse) opts.allowDiskUse = true;
	if (q.let) {
		const l = parseEjson(c.fn.getNode(), q.let, 'let', c.i);
		if (l && Object.keys(l).length) opts.let = l;
	}
	return { opts, explain: String(q.explain ?? '') };
}

const writeOpts = (c: Ctx): any => {
	const w = p(c, 'writeOptions', {}) as IDataObject;
	const o: any = { ...sess(c) };
	if (w.bypassDocumentValidation) o.bypassDocumentValidation = true;
	if (w.arrayFilters) o.arrayFilters = parseEjson(c.fn.getNode(), w.arrayFilters, 'arrayFilters', c.i);
	if (w.ordered === false) o.ordered = false;
	return o;
};

const idOf = (v: string): ObjectId | string => (HEX24.test(v) ? new ObjectId(v) : v);

const bulkSummary = (r: any) => ({
	acknowledged: r.acknowledged,
	insertedCount: r.insertedCount,
	matchedCount: r.matchedCount,
	modifiedCount: r.modifiedCount,
	deletedCount: r.deletedCount,
	upsertedCount: r.upsertedCount,
	...(r.upsertedIds ? { upsertedIds: r.upsertedIds } : {}),
	...(r.insertedIds ? { insertedIds: r.insertedIds } : {}),
});

const isSearch = (c: Ctx) => p(c, 'searchIndexName', 'default');

export function runsOnce(fn: IExecuteFunctions, resource: string, operation: string): boolean {
	if (resource === 'bulk') {
		return operation === 'bulkUpsert' || (operation === 'bulkWrite' && fn.getNodeParameter('bulkSource', 0, 'json') === 'input');
	}
	return resource === 'document' && operation === 'insertMany' && fn.getNodeParameter('documentSource', 0, 'json') === 'input';
}

const handlers: Record<string, Handler> = {
	// ---------------- document ----------------
	'document.find': async (c) => {
		const { opts, explain } = queryOptions(c);
		const projection = pj(c, 'projection');
		const sort = pj(c, 'sort');
		const cursor = coll(c).find(pj(c, 'filter') ?? {}, {
			...opts,
			...(projection ? { projection } : {}),
			...(sort ? { sort } : {}),
			skip: p(c, 'skip', 0) || undefined,
			limit: p(c, 'returnAll', false) ? undefined : p(c, 'limit', 50),
		});
		return explain ? one(await cursor.explain(explain as any)) : wrap(await cursor.toArray());
	},
	'document.findOne': async (c) => {
		const { opts } = queryOptions(c);
		const projection = pj(c, 'projection');
		const sort = pj(c, 'sort');
		const doc = await coll(c).findOne(pj(c, 'filter') ?? {}, {
			...opts,
			...(projection ? { projection } : {}),
			...(sort ? { sort } : {}),
			skip: p(c, 'skip', 0) || undefined,
		});
		return doc ? one(doc) : [];
	},
	'document.insertOne': async (c) => {
		const doc =
			p(c, 'documentSource', 'json') === 'input' ? conv(c, c.i) : pj(c, 'document');
		const r = await coll(c).insertOne(doc as any, writeOpts(c));
		return one({ acknowledged: r.acknowledged, insertedId: r.insertedId });
	},
	'document.insertMany': async (c) => {
		const docs =
			p(c, 'documentSource', 'json') === 'input'
				? c.items.map((_, idx) => conv(c, idx))
				: pj(c, 'documents');
		if (!Array.isArray(docs)) throw new NodeOperationError(c.fn.getNode(), '"Documents" must be a JSON array');
		const r = await coll(c).insertMany(docs as any[], writeOpts(c));
		return one({ acknowledged: r.acknowledged, insertedCount: r.insertedCount, insertedIds: Object.values(r.insertedIds) });
	},
	'document.updateOne': async (c) => {
		const r = await coll(c).updateOne(pj(c, 'filter') ?? {}, pj(c, 'update'), {
			...writeOpts(c),
			upsert: p(c, 'upsert', false),
			...(pj(c, 'sort') ? { sort: pj(c, 'sort') } : {}),
		});
		return one(r);
	},
	'document.updateMany': async (c) =>
		one(await coll(c).updateMany(pj(c, 'filter') ?? {}, pj(c, 'update'), { ...writeOpts(c), upsert: p(c, 'upsert', false) })),
	'document.replaceOne': async (c) =>
		one(await coll(c).replaceOne(pj(c, 'filter') ?? {}, pj(c, 'replacement'), { ...writeOpts(c), upsert: p(c, 'upsert', false) })),
	'document.deleteOne': async (c) => one(await coll(c).deleteOne(pj(c, 'filter') ?? {}, writeOpts(c))),
	'document.deleteMany': async (c) => one(await coll(c).deleteMany(pj(c, 'filter') ?? {}, writeOpts(c))),
	'document.findOneAndUpdate': async (c) => {
		const projection = pj(c, 'projection');
		const sort = pj(c, 'sort');
		const doc = await coll(c).findOneAndUpdate(pj(c, 'filter') ?? {}, pj(c, 'update'), {
			...writeOpts(c),
			upsert: p(c, 'upsert', false),
			returnDocument: p(c, 'returnDocument', 'after'),
			...(projection ? { projection } : {}),
			...(sort ? { sort } : {}),
		});
		return doc ? one(doc) : [];
	},
	'document.findOneAndReplace': async (c) => {
		const projection = pj(c, 'projection');
		const sort = pj(c, 'sort');
		const doc = await coll(c).findOneAndReplace(pj(c, 'filter') ?? {}, pj(c, 'replacement'), {
			...writeOpts(c),
			upsert: p(c, 'upsert', false),
			returnDocument: p(c, 'returnDocument', 'after'),
			...(projection ? { projection } : {}),
			...(sort ? { sort } : {}),
		});
		return doc ? one(doc) : [];
	},
	'document.findOneAndDelete': async (c) => {
		const projection = pj(c, 'projection');
		const sort = pj(c, 'sort');
		const doc = await coll(c).findOneAndDelete(pj(c, 'filter') ?? {}, {
			...writeOpts(c),
			...(projection ? { projection } : {}),
			...(sort ? { sort } : {}),
		});
		return doc ? one(doc) : [];
	},
	'document.count': async (c) => {
		const { opts } = queryOptions(c);
		return one({ count: await coll(c).countDocuments(pj(c, 'filter') ?? {}, opts) });
	},
	'document.estimatedCount': async (c) => one({ count: await coll(c).estimatedDocumentCount(sess(c)) }),
	'document.distinct': async (c) => {
		const { opts } = queryOptions(c);
		return one({ values: await coll(c).distinct(p(c, 'distinctField'), pj(c, 'filter') ?? {}, opts) });
	},

	// ---------------- aggregation ----------------
	'aggregation.aggregate': async (c) => {
		const pipeline = pj(c, 'pipeline');
		if (!Array.isArray(pipeline)) throw new NodeOperationError(c.fn.getNode(), '"Pipeline" must be a JSON array');
		const { opts, explain } = queryOptions(c);
		const cursor = coll(c).aggregate(pipeline, opts);
		return explain ? one(await cursor.explain(explain as any)) : wrap(await cursor.toArray());
	},
	'database.databaseAggregate': async (c) => {
		const pipeline = pj(c, 'pipeline');
		if (!Array.isArray(pipeline)) throw new NodeOperationError(c.fn.getNode(), '"Pipeline" must be a JSON array');
		const { opts } = queryOptions(c);
		return wrap(await c.db.aggregate(pipeline, opts).toArray());
	},

	// ---------------- bulk ----------------
	'bulk.bulkWrite': async (c) => {
		const ops =
			p(c, 'bulkSource', 'json') === 'input'
				? c.items.map((_, idx) => conv(c, idx))
				: pj(c, 'bulkOperations');
		if (!Array.isArray(ops)) throw new NodeOperationError(c.fn.getNode(), '"Operations" must be a JSON array');
		return one(bulkSummary(await coll(c).bulkWrite(ops as any[], { ordered: p(c, 'ordered', true), ...sess(c) })));
	},
	'bulk.bulkUpsert': async (c) => {
		const keys = [...toFieldSet(p(c, 'matchFields', '_id'))];
		const replace = p(c, 'upsertMode', 'set') === 'replace';
		const ops = c.items.map((it, idx) => {
			const doc = conv(c, idx) as Record<string, any>;
			const filter: Record<string, any> = {};
			for (const k of keys) {
				if (doc[k] === undefined) {
					throw new NodeOperationError(c.fn.getNode(), `Match field "${k}" missing on item ${idx}`, { itemIndex: idx });
				}
				filter[k] = doc[k];
			}
			if (replace) return { replaceOne: { filter, replacement: doc, upsert: true } };
			const set = { ...doc };
			if (keys.includes('_id')) delete set._id;
			return { updateOne: { filter, update: { $set: set }, upsert: true } };
		});
		return one(bulkSummary(await coll(c).bulkWrite(ops as any[], { ordered: p(c, 'ordered', true), ...sess(c) })));
	},
	'database.clientBulkWrite': async (c) => {
		const models = pj(c, 'clientBulkOperations');
		if (!Array.isArray(models)) throw new NodeOperationError(c.fn.getNode(), '"Operations" must be a JSON array');
		const r: any = await c.client.bulkWrite(models as any[], { ordered: p(c, 'ordered', true), ...sess(c) });
		return one(bulkSummary(r));
	},

	// ---------------- collection ----------------
	'collection.create': async (c) => {
		const name = p(c, 'collection');
		await c.db.createCollection(name, { ...(pj(c, 'createOptions') ?? {}), ...sess(c) });
		return one({ ok: 1, collection: name });
	},
	'collection.createView': async (c) => {
		const name = p(c, 'collection');
		await c.db.createCollection(name, { viewOn: p(c, 'viewOn'), pipeline: pj(c, 'viewPipeline'), ...sess(c) });
		return one({ ok: 1, view: name });
	},
	'collection.drop': async (c) => one({ dropped: await coll(c).drop(sess(c)) }),
	'collection.modify': async (c) =>
		one(await c.db.command({ collMod: p(c, 'collection'), ...(pj(c, 'modifyOptions') ?? {}) }, sess(c))),
	'collection.rename': async (c) => {
		const newName = p(c, 'newName');
		await coll(c).rename(newName, { dropTarget: p(c, 'dropTarget', false), ...sess(c) });
		return one({ renamed: true, to: newName });
	},
	'collection.stats': async (c) =>
		wrap(await coll(c).aggregate([{ $collStats: { storageStats: {}, count: {} } }], sess(c)).toArray()),

	// ---------------- index ----------------
	'index.create': async (c) => {
		const name = await coll(c).createIndex(pj(c, 'indexKeys'), { ...(pj(c, 'indexOptions') ?? {}), ...sess(c) });
		return one({ indexName: name });
	},
	'index.createMany': async (c) => {
		const names = await coll(c).createIndexes(pj(c, 'indexes'), sess(c));
		return one({ indexNames: names });
	},
	'index.drop': async (c) => one(await coll(c).dropIndex(p(c, 'indexName'), sess(c))),
	'index.dropAll': async (c) => one({ dropped: await coll(c).dropIndexes(sess(c)) }),
	'index.list': async (c) => wrap(await coll(c).indexes(sess(c))),
	'index.stats': async (c) => wrap(await coll(c).aggregate([{ $indexStats: {} }], sess(c)).toArray()),

	// ---------------- search ----------------
	'search.createSearchIndex': async (c) => {
		const name = await coll(c).createSearchIndex({
			name: isSearch(c),
			type: p(c, 'searchIndexType', 'search'),
			definition: pj(c, 'searchDefinition'),
		});
		return one({ indexName: name });
	},
	'search.updateSearchIndex': async (c) => {
		await coll(c).updateSearchIndex(isSearch(c), pj(c, 'searchDefinition'));
		return one({ updated: true, indexName: isSearch(c) });
	},
	'search.dropSearchIndex': async (c) => {
		await coll(c).dropSearchIndex(isSearch(c));
		return one({ dropped: true, indexName: isSearch(c) });
	},
	'search.listSearchIndexes': async (c) => wrap(await coll(c).listSearchIndexes().toArray()),
	'search.atlasSearch': async (c) => {
		const pipeline = [
			{ $search: { index: isSearch(c), ...pj(c, 'searchQuery') } },
			{ $addFields: { _searchScore: { $meta: 'searchScore' } } },
			{ $limit: p(c, 'searchLimit', 10) },
		];
		return wrap(await coll(c).aggregate(pipeline, sess(c)).toArray());
	},
	'search.vectorSearch': async (c) => {
		const exact = p(c, 'vectorExact', false);
		const filter = pj(c, 'vectorFilter');
		const pipeline = [
			{
				$vectorSearch: {
					index: isSearch(c),
					path: p(c, 'vectorPath'),
					queryVector: pj(c, 'queryVector'),
					limit: p(c, 'searchLimit', 10),
					...(exact ? { exact: true } : { numCandidates: p(c, 'numCandidates', 100) }),
					...(filter ? { filter } : {}),
				},
			},
			{ $addFields: { _vectorScore: { $meta: 'vectorSearchScore' } } },
		];
		return wrap(await coll(c).aggregate(pipeline, sess(c)).toArray());
	},

	// ---------------- gridfs ----------------
	'gridfs.upload': async (c) => {
		const bucket = new GridFSBucket(c.db, { bucketName: p(c, 'bucketName', 'fs') });
		const prop = p(c, 'binaryPropertyName', 'data');
		const buffer = await c.fn.helpers.getBinaryDataBuffer(c.i, prop);
		const meta = c.items[c.i].binary?.[prop];
		const filename = p(c, 'fileName') || meta?.fileName || 'file';
		const metadata = { ...(meta?.mimeType ? { mimeType: meta.mimeType } : {}), ...(pj(c, 'gridfsMetadata') ?? {}) };
		const stream = bucket.openUploadStream(filename, { metadata });
		await new Promise<void>((resolve, reject) => {
			stream.once('finish', () => resolve()).once('error', reject);
			stream.end(buffer);
		});
		return one({ fileId: stream.id, filename, length: buffer.length });
	},
	'gridfs.download': async (c) => {
		const bucketName = p(c, 'bucketName', 'fs');
		const bucket = new GridFSBucket(c.db, { bucketName });
		const fileId = p(c, 'fileId');
		const filename = p(c, 'fileName');
		if (!fileId && !filename) throw new NodeOperationError(c.fn.getNode(), 'Provide a File ID or a File Name');
		const files = c.db.collection(`${bucketName}.files`);
		const doc = fileId
			? await files.findOne({ _id: idOf(fileId) as any })
			: await files.findOne({ filename }, { sort: { uploadDate: -1 } });
		if (!doc) throw new NodeOperationError(c.fn.getNode(), 'File not found in GridFS');
		const chunks: Buffer[] = [];
		for await (const chunk of bucket.openDownloadStream(doc._id as any)) chunks.push(chunk as Buffer);
		const binary = await c.fn.helpers.prepareBinaryData(Buffer.concat(chunks), doc.filename, doc.metadata?.mimeType);
		return [{ json: asObject(doc), binary: { [p(c, 'binaryPropertyNameOut', 'data')]: binary } }];
	},
	'gridfs.list': async (c) => {
		const bucket = new GridFSBucket(c.db, { bucketName: p(c, 'bucketName', 'fs') });
		return wrap(await bucket.find(pj(c, 'gridfsFilter') ?? {}).toArray());
	},
	'gridfs.delete': async (c) => {
		const bucket = new GridFSBucket(c.db, { bucketName: p(c, 'bucketName', 'fs') });
		await bucket.delete(idOf(p(c, 'fileId')) as any);
		return one({ deleted: true, fileId: p(c, 'fileId') });
	},
	'gridfs.rename': async (c) => {
		const bucket = new GridFSBucket(c.db, { bucketName: p(c, 'bucketName', 'fs') });
		await bucket.rename(idOf(p(c, 'fileId')) as any, p(c, 'newFileName'));
		return one({ renamed: true, fileId: p(c, 'fileId'), filename: p(c, 'newFileName') });
	},

	// ---------------- database ----------------
	'database.listDatabases': async (c) => wrap((await c.client.db().admin().listDatabases()).databases),
	'database.dropDatabase': async (c) => one({ dropped: await c.db.dropDatabase(sess(c)) }),
	'database.listCollections': async (c) =>
		wrap(await c.db.listCollections(pj(c, 'collectionFilter') ?? {}, { nameOnly: p(c, 'nameOnly', false), ...sess(c) } as any).toArray()),
	'database.runCommand': async (c) => one(await c.db.command(pj(c, 'command'), sess(c))),
	'database.ping': async (c) => one(await c.db.command({ ping: 1 })),
	'database.buildInfo': async (c) => one(await c.db.command({ buildInfo: 1 })),
	'database.queryStats': async (c) =>
		wrap(await c.client.db('admin').aggregate([{ $queryStats: pj(c, 'queryStatsOptions') ?? {} }]).toArray()),
	'database.currentOp': async (c) =>
		wrap(await c.client.db('admin').aggregate([{ $currentOp: pj(c, 'currentOpOptions') ?? {} }]).toArray()),
};

export async function runOperation(c: Ctx, resource: string, operation: string): Promise<Out[]> {
	const handler = handlers[`${resource}.${operation}`];
	if (!handler) throw new NodeOperationError(c.fn.getNode(), `Unsupported operation: ${resource}.${operation}`);
	return handler(c);
}
