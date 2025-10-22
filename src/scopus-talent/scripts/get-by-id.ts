import { parseArgs } from 'jsr:@std/cli@1.0.23';
import {
  AdditionalDataFn,
  generateExtractAllFn,
} from '../extractors/extract-all.ts';
import { generate } from '../generator.ts';

const apiKey = Deno.env.get('ELSEVIER_KEY');

if (!apiKey) {
  console.error(
    `API key must be specified by environment variable 'ELSEVIER_KEY'`,
  );
  Deno.exit(-1);
}

const apiKeys = apiKey.split(/\s*,\s*/gi).filter((value) => value !== '');

const parsedArgs = parseArgs(Deno.args, {
  string: ['target', 'additional-data-fn'],
});

if (parsedArgs['_'].length === 0) {
  console.error(`Configuration name is required.`);
  Deno.exit(-1);
}

const configName = `${parsedArgs['_'][0]}`;

const additionalDataFn = (
  parsedArgs['additional-data-fn']
    ? (await import(parsedArgs['additional-data-fn'])).default
    : undefined
) as AdditionalDataFn | undefined;

const targetDir =
  (!parsedArgs['target'] ? undefined : parsedArgs['target']) ?? './target';

await generate(
  configName,
  targetDir,
  apiKeys,
  generateExtractAllFn({ additionalDataFn }),
);
