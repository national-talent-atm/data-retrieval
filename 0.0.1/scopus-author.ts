import { TextLineStream } from 'https://deno.land/std@0.208.0/streams/mod.ts';
import { ScopusClient } from './elsevier-clients/scopus-client.ts';
import { filter, map } from './streams.ts';
import { ScopusAuthorRetrievalApi } from './elsevier-apis/scopus-author-retrieval-api.ts';
import { ScopusAuthorResponseBody } from './elsevier-types/scopus-author-types.ts';
import { readerToAsyncIterable } from './utils.ts';

const getFileName = (fileId: string) => {
  return `au-id-${fileId}.json` as const;
};

const apiKey = Deno.env.get('ELSEVIER_KEY');

if (!apiKey) {
  console.error(
    `API key must be specified by environment variable 'ELSEVIER_KEY'`,
  );
  Deno.exit(-1);
}

const apiKeys = apiKey.split(/\s*,\s*/gi).filter((value) => value !== '');

const configName = 'au-qt-2023-12-07';

const inputFile = `./target/${configName}.txt` as const;
const outputDir = `./target/output/${configName}` as const;
const catchDir = outputDir;

const getCache = (fileId: string) => {
  return `${catchDir}/${getFileName(`${fileId}`)}` as const;
};

const getOuput = (fileId: string) => {
  return `${outputDir}/${getFileName(`${fileId}`)}` as const;
};

await Deno.mkdir(outputDir, { recursive: true });

const client = new ScopusClient(apiKeys, 10);
const authorRetrievalApi = new ScopusAuthorRetrievalApi(client);

let count = 1;

const stream = ReadableStream.from(
  readerToAsyncIterable(await Deno.open(inputFile, { read: true })),
)
  .pipeThrough(new TextDecoderStream())
  .pipeThrough(new TextLineStream())
  .pipeThrough(map((value) => value.trim()))
  .pipeThrough(filter((value) => value !== ''))
  .pipeThrough(map((value) => value.split('\t').map((term) => term.trim())))
  .pipeThrough(
    map(async ([id, name, ind]) => {
      const index = `${count++}`.padStart(5, ' ');
      console.info(`start [${index}]:`, id, name, ind);
      const query = `AU-ID(${id})`;
      console.info(`\t[${index}] loading: ${query}`);

      try {
        if (id === '') {
          throw new Error(`The scopus-id for "${name}" is empty`);
        }

        let isCached = true;

        const authorResult = await (async () => {
          try {
            return JSON.parse(
              await Deno.readTextFile(getCache(id)),
            ) as ScopusAuthorResponseBody;
          } catch {
            isCached = false;
            return await authorRetrievalApi.authorId(
              id,
              {
                view: 'ENHANCED',
              },
              (limit, remaining, reset, status) =>
                console.info(
                  `\t[${index}] rateLimit: ${remaining?.padStart(
                    5,
                    ' ',
                  )}/${limit} reset: ${reset} [${status}]`,
                ),
            );
          }
        })();

        console.info(`\t[${index}] loaded`);
        return { index, id, name, ind, isCached, authorResult };
      } catch (err) {
        console.error(`\t[${index}] error`, err);
        return {
          index,
          id,
          name,
          ind,
          isCached: false,
          authorResult:
            err instanceof Error
              ? new Error(err.message, {
                  cause: {
                    query: query,
                    error: err.cause,
                    origin: err,
                  },
                })
              : new Error(`${err}`, {
                  cause: {
                    query: query,
                    origin: err,
                  },
                }),
        };
      }
    }),
  );

stream.pipeTo(
  new WritableStream({
    async write({ index, id, name, isCached, authorResult }) {
      if (isCached && getCache(id) === getOuput(id)) {
        console.info(`\t[${index}] done: cached`);
        return;
      }

      const isError = authorResult instanceof Error;
      const encoder = new TextEncoder();
      const dataArray = encoder.encode(
        JSON.stringify(
          isError
            ? {
                name: authorResult.name,
                message: authorResult.message,
                cause: authorResult.cause,
              }
            : authorResult,
          undefined,
          2,
        ),
      );

      console.info(`\t[${index}] writing to file`);
      const indexPrefix = `inx${index.replaceAll(' ', '0')}`;
      const fileId = id === '' ? `${indexPrefix}-${name}` : id;
      const prefix = isError ? `error-${indexPrefix}-` : '';
      const fpOut = await Deno.open(getOuput(`${prefix}${fileId}`), {
        create: true,
        write: true,
      });
      await fpOut.write(dataArray);
      fpOut.close();
      console.info(`\t[${index}] done`);
    },
  }),
);
