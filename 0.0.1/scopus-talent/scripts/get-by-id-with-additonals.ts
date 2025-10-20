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

const configName = '20250706-13-cmu-health-400-rows';
const targetDir = './target';

await generate(
  configName,
  targetDir,
  apiKeys,
  generateExtractAllFn({
    additionalDataFn: ({ rest }) => {
      const [x_name, x_field, x_university] = rest;

      return {
        x_name,
        x_field,
        x_university,
      };
    },
  }),
);
