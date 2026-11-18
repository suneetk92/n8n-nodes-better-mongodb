 
import { NodeConnectionTypes, NodeOperationError } from 'n8n-workflow';
import type {
	IDataObject,
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import type { ClientSession } from 'mongodb';
import { getClient, resolveDb, serialize, toFieldSet, toFieldSetCI } from './GenericFunctions';
import type { OutputFormat } from './GenericFunctions';
import { properties } from './descriptions';
import { runOperation, runsOnce } from './operations';
import type { Ctx } from './operations';

export class BetterMongoDb implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Better MongoDB',
		name: 'betterMongoDb',
		icon: { light: 'file:betterMongoDb.svg', dark: 'file:betterMongoDb.dark.svg' },
		group: ['input'],
		version: 1,
		subtitle: '={{$parameter["resource"] + ": " + $parameter["operation"]}}',
		description:
			'Full-featured MongoDB: CRUD, aggregation, bulk writes, transactions, indexes, Atlas/vector search, GridFS and BSON types',
		defaults: { name: 'Better MongoDB' },
		inputs: [NodeConnectionTypes.Main],
		outputs: [NodeConnectionTypes.Main],
		usableAsTool: true,
		credentials: [{ name: 'mongoDb', required: true }],
		properties,
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const { client, database } = await getClient(this);
		const useTransaction = Boolean((this.getNodeParameter('options', 0, {}) as IDataObject).transaction);

		const run = async (session?: ClientSession): Promise<INodeExecutionData[]> => {
			const returnData: INodeExecutionData[] = [];

			const execOne = async (i: number, pairedItem: INodeExecutionData['pairedItem']) => {
				const resource = this.getNodeParameter('resource', i) as string;
				const operation = this.getNodeParameter('operation', i) as string;
				const options = this.getNodeParameter('options', i, {}) as IDataObject;
				const ctx: Ctx = {
					fn: this,
					client,
					db: resolveDb(client, database, this.getNodeParameter('database', i, '') as string),
					session,
					convert: {
						idFields: toFieldSet(options.objectIdFields ?? '_id'),
						dateFields: toFieldSetCI(options.dateFields),
					},
					options,
					items,
					i,
				};
				const format = (options.outputFormat as OutputFormat) ?? 'simplified';
				const out = await runOperation(ctx, resource, operation);
				for (const o of out) {
					returnData.push({ json: serialize(o.json, format), binary: o.binary, pairedItem });
				}
			};

			const resource0 = this.getNodeParameter('resource', 0) as string;
			const operation0 = this.getNodeParameter('operation', 0) as string;
			if (runsOnce(this, resource0, operation0)) {
				await execOne(0, items.map((_, item) => ({ item })));
				return returnData;
			}

			for (let i = 0; i < items.length; i++) {
				try {
					await execOne(i, { item: i });
				} catch (error) {
					if (this.continueOnFail() && !session) {
						returnData.push({ json: { error: (error as Error).message }, pairedItem: { item: i } });
						continue;
					}
					throw new NodeOperationError(this.getNode(), error as Error, { itemIndex: i });
				}
			}
			return returnData;
		};

		try {
			if (!useTransaction) return [await run()];
			const session = client.startSession();
			let result: INodeExecutionData[] = [];
			try {
				await session.withTransaction(async () => {
					result = await run(session);
				});
			} finally {
				await session.endSession();
			}
			return [result];
		} finally {
			await client.close();
		}
	}
}
