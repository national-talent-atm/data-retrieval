import { generateExtractAllFn } from '../extractors/extract-all.ts';
import { generate } from '../generator.ts';

const apiKey = Deno.env.get('ELSEVIER_KEY');

if (!apiKey) {
  console.error(
    `API key must be specified by environment variable 'ELSEVIER_KEY'`,
  );
  Deno.exit(-1);
}

const apiKeys = apiKey.split(/\s*,\s*/gi).filter((value) => value !== '');

const configName = 'new-1323-row-id-20250513';
const targetDir = './target';

await generate(
  configName,
  targetDir,
  apiKeys,
  generateExtractAllFn({
    additionalDataFn: ({ rest }) => {
      const [lastname_en, firstname_en, initname_en] = rest;

      return { lastname_en, firstname_en, initname_en };
    },
  }),
);
