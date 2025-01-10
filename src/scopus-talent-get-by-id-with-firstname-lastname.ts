import { generate } from './scopus-talent.ts';

const apiKey = Deno.env.get('ELSEVIER_KEY');

if (!apiKey) {
  console.error(
    `API key must be specified by environment variable 'ELSEVIER_KEY'`,
  );
  Deno.exit(-1);
}

const apiKeys = apiKey.split(/\s*,\s*/gi).filter((value) => value !== '');

const configName = 'new-165-20250108';
const targetDir = './target';

await generate(configName, targetDir, apiKeys, {
  additionalDataFn: ({ rest }) => {
    const [firstname_en, lastname_en] = rest;

    return { firstname_en, lastname_en };
  },
});
