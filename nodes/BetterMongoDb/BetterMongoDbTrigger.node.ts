/* eslint-disable @typescript-eslint/no-explicit-any */
import { NodeConnectionTypes } from 'n8n-workflow';
import type { ITriggerFunctions, INodeType, INodeTypeDescription, ITriggerResponse } from 'n8n-workflow';
import { convertFields, getClient, parseEjson, resolveDb, serialize, toFieldSet } from './GenericFunctions';
import type { OutputFormat } from './GenericFunctions';

const EVENT_TYPES = [
	'insert',
	'update',
	'replace',
	'delete',
	'drop',
	'rename',
	'dropDatabase',
	'invalidate',
	'create',
	'createIndexes',
	'dropIndexes',
	'modify',
	'shardCollection',
	'refineCollectionShardKey',
	'reshardCollection',
];

export class BetterMongoDbTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Better MongoDB Trigger',
		name: 'betterMongoDbTrigger',
		icon: { light: 'file:betterMongoDb.svg', dark: 'file:betterMongoDb.dark.svg' },
		group: ['trigger'],
		version: 1,
		subtitle: '={{$parameter["scope"] + " change stream"}}',
		description: 'Starts a workflow on MongoDB change stream events (replica set or sharded cluster required)',
		defaults: { name: 'Better MongoDB Trigger' },
		inputs: [],
		outputs: [NodeConnectionTypes.Main],
		credentials: [{ name: 'mongoDb', required: true }],
		properties: [
			{
				displayName: 'Watch',
				name: 'scope',
				type: 'options',
				options: [
					{ name: 'Collection', value: 'collection' },
					{ name: 'Database', value: 'database' },
					{ name: 'Entire Cluster', value: 'cluster' },
				],
				default: 'collection',
			},
			{
				displayName: 'Database',
				name: 'database',
				type: 'string',
				default: '',
				placeholder: 'Defaults to the credential database',
				displayOptions: { show: { scope: ['collection', 'database'] } },
			},
			{
				displayName: 'Collection',
				name: 'collection',
				type: 'string',
				required: true,
				default: '',
				displayOptions: { show: { scope: ['collection'] } },
			},
			{
				displayName: 'Event Types',
				name: 'eventTypes',
				type: 'multiOptions',
				options: EVENT_TYPES.map((v) => ({ name: v, value: v })),
				default: ['insert', 'update', 'replace', 'delete'],
				description: 'Which operation types trigger the workflow. Leave empty for all. DDL events (create, modify, ...) need "Show Expanded Events".',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Additional Pipeline',
						name: 'pipeline',
						type: 'json',
						default: '[]',
						description: 'Extra change stream stages ($match, $project, $addFields, $redact, $set, $unset, $replaceRoot), e.g. [{"$match": {"fullDocument.status": "paid"}}]',
					},
					{
						displayName: 'Convert ObjectId Fields (In Pipeline)',
						name: 'objectIdFields',
						type: 'string',
						default: '_id',
					},
					{
						displayName: 'Full Document',
						name: 'fullDocument',
						type: 'options',
						options: [
							{ name: 'Default (Delta Only for Updates)', value: 'default' },
							{ name: 'Update Lookup', value: 'updateLookup' },
							{ name: 'When Available', value: 'whenAvailable' },
							{ name: 'Required', value: 'required' },
						],
						default: 'updateLookup',
						description: 'Whenavailable/required use post-images (collection needs changeStreamPreAndPostImages)',
					},
					{
						displayName: 'Full Document Before Change',
						name: 'fullDocumentBeforeChange',
						type: 'options',
						options: [
							{ name: 'Off', value: 'off' },
							{ name: 'When Available', value: 'whenAvailable' },
							{ name: 'Required', value: 'required' },
						],
						default: 'off',
						description: 'Include the pre-image. Needs changeStreamPreAndPostImages enabled on the collection.',
					},
					{
						displayName: 'Max Await Time (Ms)',
						name: 'maxAwaitTimeMS',
						type: 'number',
						default: 1000,
					},
					{
						displayName: 'Output Format',
						name: 'outputFormat',
						type: 'options',
						options: [
							{ name: 'Simplified', value: 'simplified' },
							{ name: 'Extended JSON (Relaxed)', value: 'relaxed' },
							{ name: 'Extended JSON (Canonical)', value: 'canonical' },
						],
						default: 'simplified',
					},
					{
						displayName: 'Persist Resume Token',
						name: 'persistResumeToken',
						type: 'boolean',
						default: true,
						description: 'Whether to continue where the workflow left off after a restart (production runs only)',
					},
					{
						displayName: 'Show Expanded Events',
						name: 'showExpandedEvents',
						type: 'boolean',
						default: false,
						description: 'Whether to emit DDL events (create, createIndexes, modify, ...)',
					},
				],
			},
		],
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		const scope = this.getNodeParameter('scope') as string;
		const options = this.getNodeParameter('options', {}) as Record<string, any>;
		const eventTypes = this.getNodeParameter('eventTypes', []) as string[];
		const format = (options.outputFormat as OutputFormat) ?? 'simplified';
		const isManual = this.getMode() === 'manual';
		const staticData = this.getWorkflowStaticData('node') as Record<string, any>;

		const extra = parseEjson(this.getNode(), options.pipeline ?? this.getNodeParameter('pipeline', '[]'), 'pipeline', 0);
		const pipeline: any[] = [
			...(eventTypes.length ? [{ $match: { operationType: { $in: eventTypes } } }] : []),
			...((convertFields(extra ?? [], { idFields: toFieldSet(options.objectIdFields ?? '_id'), dateFields: new Set() }) as any[]) ?? []),
		];

		const { client, database } = await getClient(this);
		const watchOptions: Record<string, any> = {};
		if (options.fullDocument && options.fullDocument !== 'default') watchOptions.fullDocument = options.fullDocument;
		if (options.fullDocumentBeforeChange && options.fullDocumentBeforeChange !== 'off') {
			watchOptions.fullDocumentBeforeChange = options.fullDocumentBeforeChange;
		}
		if (options.showExpandedEvents) watchOptions.showExpandedEvents = true;
		if (options.maxAwaitTimeMS) watchOptions.maxAwaitTimeMS = options.maxAwaitTimeMS;
		const persist = !isManual && options.persistResumeToken !== false;
		if (persist && staticData.resumeToken) watchOptions.resumeAfter = staticData.resumeToken;

		let target: any;
		if (scope === 'cluster') target = client;
		else {
			const db = resolveDb(client, database, this.getNodeParameter('database', '') as string);
			target = scope === 'database' ? db : db.collection(this.getNodeParameter('collection') as string);
		}

		const stream = target.watch(pipeline, watchOptions);
		let onFirst: (() => void) | undefined;

		stream.on('change', (change: any) => {
			if (persist) staticData.resumeToken = change._id;
			this.emit([this.helpers.returnJsonArray([serialize(change, format)])]);
			onFirst?.();
		});
		stream.on('error', (error: Error & { code?: number }) => {
			if (error.code === 286 || error.code === 280) delete staticData.resumeToken;
			this.emitError(error);
		});

		const manualTriggerFunction = async () => {
			await new Promise<void>((resolve) => {
				onFirst = resolve;
			});
		};

		const closeFunction = async () => {
			stream.removeAllListeners();
			await stream.close().catch(() => undefined);
			await client.close().catch(() => undefined);
		};

		return { closeFunction, manualTriggerFunction };
	}
}
