import { configWithoutCloudSupport } from '@n8n/node-cli/eslint';

export default [
	...configWithoutCloudSupport,
	{
		// The mongodb driver is a required runtime dependency, so this package targets self-hosted n8n only.
		files: ['package.json'],
		rules: { '@n8n/community-nodes/no-runtime-dependencies': 'off' },
	},
	{
		// Deliberately reuses n8n's built-in `mongoDb` credential instead of shipping its own.
		files: ['nodes/**/*.node.ts'],
		rules: { '@n8n/community-nodes/no-credential-reuse': 'off' },
	},
];
