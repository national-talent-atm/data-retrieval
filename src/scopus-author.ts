import {
  readableStreamFromReader,
  TextLineStream,
} from 'https://deno.land/std@0.186.0/streams/mod.ts';
import { ScopusClient } from './elsevier-clients/scopus-client.ts';
import { filter, map } from './streams.ts';
import { ScopusAuthorRetrievalApi } from './elsevier-apis/scopus-author-retrieval-api.ts';

const apiKey = Deno.env.get('ELSEVIER_KEY');

if (!apiKey) {
  console.error(
    `API key must be specified by environment variable 'ELSEVIER_KEY'`,
  );
  Deno.exit(-1);
}

const inputFile = './target/test.txt';
const outputDir = './target/output';

const client = new ScopusClient(apiKey, 10);
const authorRetrievalApi = new ScopusAuthorRetrievalApi(client);

const stream = readableStreamFromReader(
  await Deno.open(inputFile, { read: true }),
)
  .pipeThrough(new TextDecoderStream())
  .pipeThrough(new TextLineStream())
  .pipeThrough(map((value) => value.trim()))
  .pipeThrough(filter((value) => value !== ''))
  .pipeThrough(
    map(async (line) => {
      console.info('start:', line);
      console.info('\tloading');
      const author = await authorRetrievalApi.authorId(line);
      console.info('\tloaded');
      return [line, author] as const;
    }),
  );

stream.pipeTo(
  new WritableStream({
    async write([line, author]) {
      const encoder = new TextEncoder();
      const dataArray = encoder.encode(JSON.stringify(author, undefined, 2));

      console.info('\twriting to file');
      const fpOut = await Deno.open(`${outputDir}/au-id-${line}.json`, {
        create: true,
        write: true,
      });
      await fpOut.write(dataArray);
      fpOut.close();
      console.info('\tdone');
    },
  }),
);
