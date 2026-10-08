/* eslint-disable @typescript-eslint/no-explicit-any */
import { createSecureContext } from 'tls';
import { BSON, MongoClient, ObjectId } from 'mongodb';
import type { Db } from 'mongodb';
import { NodeOperationError } from 'n8n-workflow';
import type { ICredentialDataDecryptedObject, IExecuteFunctions, INode, ITriggerFunctions } from 'n8n-workflow';

export type OutputFormat = 'simplified' | 'relaxed' | 'canonical';

export interface ConvertOptions {
	idFields: Set<string>;
	dateFields: Set<string>;
}

const HEX24 = /^[0-9a-fA-F]{24}$/;
const { EJSON } = BSON;

function formatPem(value: string): string {
	const text = value.replace(/\\n/g, '\n').trim();
	if (!text.includes('-----BEGIN') || text.includes('\n')) return text;
	const match = text.match(/(-----BEGIN [A-Z ]+-----)(.*)(-----END [A-Z ]+-----)/s);
	if (!match) return text;
	const body = match[2].replace(/\s+/g, '').match(/.{1,64}/g)?.join('\n') ?? '';
	return `${match[1]}\n${body}\n${match[3]}`;
}

export function buildConnectionString(credentials: ICredentialDataDecryptedObject): string {
	if (credentials.configurationType === 'connectionString') {
		const cs = String(credentials.connectionString ?? '').trim();
		if (!cs) throw new Error('MongoDB credential: connection string is empty');
		return cs;
	}
	const user = String(credentials.user ?? '');
	const password = String(credentials.password ?? '');
	const auth = user ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
	const host = String(credentials.host ?? 'localhost');
	return credentials.port
		? `mongodb://${auth}${host}:${credentials.port}`
		: `mongodb+srv://${auth}${host}`;
}

export async function getClient(
	ctx: IExecuteFunctions | ITriggerFunctions,
): Promise<{ client: MongoClient; database: string }> {
	const credentials = await ctx.getCredentials('mongoDb');
	const connectionString = buildConnectionString(credentials);
	const options: Record<string, unknown> = {
		appName: 'n8n-better-mongodb',
		driverInfo: { name: 'n8n_better_mongodb' },
	};
	if (credentials.tls) {
		options.tls = true;
		if (credentials.ca || credentials.cert || credentials.key) {
			options.secureContext = createSecureContext({
				ca: credentials.ca ? formatPem(String(credentials.ca)) : undefined,
				cert: credentials.cert ? formatPem(String(credentials.cert)) : undefined,
				key: credentials.key ? formatPem(String(credentials.key)) : undefined,
				passphrase: credentials.passphrase ? String(credentials.passphrase) : undefined,
			});
		}
	}
	const client = await MongoClient.connect(connectionString, options);
	return { client, database: String(credentials.database ?? '').trim() };
}

export function resolveDb(client: MongoClient, credentialDb: string, override: string): Db {
	const name = (override || credentialDb).trim();
	return client.db(name || undefined);
}

export function parseEjson(node: INode, value: unknown, field: string, itemIndex: number): any {
	try {
		if (typeof value === 'string') {
			const text = value.trim();
			return text ? EJSON.parse(text, { relaxed: false }) : undefined;
		}
		if (value === undefined || value === null) return undefined;
		return EJSON.parse(JSON.stringify(value), { relaxed: false });
	} catch (error) {
		throw new NodeOperationError(node, `Invalid JSON / Extended JSON in "${field}": ${(error as Error).message}`, {
			itemIndex,
		});
	}
}

const isPlain = (v: unknown): v is Record<string, unknown> =>
	v !== null && typeof v === 'object' && Object.getPrototypeOf(v) === Object.prototype;

function mapDeep(v: unknown, fn: (x: unknown) => unknown): unknown {
	if (Array.isArray(v)) return v.map((x) => mapDeep(x, fn));
	if (isPlain(v)) return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, mapDeep(x, fn)]));
	return fn(v);
}

const toObjectId = (v: unknown) => (typeof v === 'string' && HEX24.test(v) ? new ObjectId(v) : v);
const toDate = (v: unknown) => {
	if (typeof v !== 'string') return v;
	const d = new Date(v);
	return Number.isNaN(d.getTime()) ? v : d;
};

export function convertFields(value: unknown, opts: ConvertOptions): unknown {
	if (!opts.idFields.size && !opts.dateFields.size) return value;
	if (Array.isArray(value)) return value.map((x) => convertFields(x, opts));
	if (isPlain(value)) {
		const out: Record<string, unknown> = {};
		for (const [k, v] of Object.entries(value)) {
			if (opts.idFields.has(k)) out[k] = mapDeep(v, toObjectId);
			else if (opts.dateFields.has(k)) out[k] = mapDeep(v, toDate);
			else out[k] = convertFields(v, opts);
		}
		return out;
	}
	return value;
}

export function toFieldSet(csv: unknown): Set<string> {
	return new Set(
		String(csv ?? '')
			.split(',')
			.map((s) => s.trim())
			.filter(Boolean),
	);
}

function simplify(v: any): any {
	if (v === null || v === undefined) return v;
	if (Array.isArray(v)) return v.map(simplify);
	if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString();
	if (Buffer.isBuffer(v)) return v.toString('base64');
	if (v instanceof RegExp) return v.toString();
	if (typeof v === 'bigint') return v.toString();
	if (typeof v === 'object' && '_bsontype' in v) {
		switch (v._bsontype) {
			case 'ObjectId':
				return v.toHexString();
			case 'Decimal128':
				return v.toString();
			case 'Long': {
				const n = v.toNumber();
				return Number.isSafeInteger(n) ? n : v.toString();
			}
			case 'Int32':
			case 'Double':
				return v.valueOf();
			case 'Binary':
				if (v.sub_type === 4) {
					try {
						return v.toUUID().toHexString(true);
					} catch {
						/* fall through to base64 */
					}
				}
				return v.toString('base64');
			case 'Timestamp':
				return { t: v.high, i: v.low };
			case 'MinKey':
				return '$minKey';
			case 'MaxKey':
				return '$maxKey';
			case 'Code':
				return v.code;
			case 'BSONRegExp':
				return `/${v.pattern}/${v.options}`;
			case 'DBRef':
				return { $ref: v.collection, $id: simplify(v.oid), ...(v.db ? { $db: v.db } : {}) };
			case 'BSONSymbol':
				return v.valueOf();
			default:
				return typeof v.toJSON === 'function' ? v.toJSON() : String(v);
		}
	}
	if (typeof v === 'object') {
		return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, simplify(x)]));
	}
	return v;
}

export function serialize(value: unknown, format: OutputFormat): any {
	if (value === null || value === undefined) return value;
	if (format === 'simplified') return simplify(value);
	const wrapped = EJSON.serialize({ v: value } as any, { relaxed: format === 'relaxed' }) as any;
	return wrapped.v;
}

export function asObject(value: unknown): Record<string, any> {
	const s = Array.isArray(value) || value === null || typeof value !== 'object' ? { value } : value;
	return s as Record<string, any>;
}
