import { stringify } from 'jsr:@std/csv@1.0.6';
import { TextLineStream } from 'jsr:@std/streams@1.0.13';
import { ScopusAuthorSearchApi } from './elsevier-apis/scopus-author-search-api.ts';
import { ScopusSearchApi } from './elsevier-apis/scopus-search-api.ts';
import { ScopusClient } from './elsevier-clients/scopus-client.ts';
import { ScopusAuthorSearchEntry } from './elsevier-types/scopus-author-search-types.ts';
import { ScopusSearchEntry } from './elsevier-types/scopus-search-types.ts';
import { ScopusSearchResponseBody } from './elsevier-types/scopus-types.ts';
import { filter, flatMap, map, toArray } from './streams.ts';
import { readerToAsyncIterable } from './utils.ts';

const getFileName = (fileId: string) => {
  return `co-au-id-${fileId}.json` as const;
};

const getPairFileName = (fileId: string) => {
  return `pair-co-au-id-${fileId}.json` as const;
};

const apiKey = Deno.env.get('ELSEVIER_KEY');

if (!apiKey) {
  console.error(
    `API key must be specified by environment variable 'ELSEVIER_KEY'`,
  );
  Deno.exit(-1);
}

const apiKeys = apiKey.split(/\s*,\s*/gi).filter((value) => value !== '');

const configName = 'co-au-ai';
//const limit = 40;
//const sortedBy = '-document-count';

const inputFile = `./target/${configName}.txt` as const;
const outputDir = `./target/output/${configName}` as const;
const catchDir = outputDir;

const getCache = (fileId: string) => {
  return `${catchDir}/${getFileName(`${fileId}`)}` as const;
};

const getPairCache = (fileId: string) => {
  return `${catchDir}/${getPairFileName(`${fileId}`)}` as const;
};

const getOuput = (fileId: string) => {
  return `${outputDir}/${getFileName(`${fileId}`)}` as const;
};

const getPairOuput = (fileId: string) => {
  return `${outputDir}/${getPairFileName(`${fileId}`)}` as const;
};

await Deno.mkdir(outputDir, { recursive: true });

const client = new ScopusClient(apiKeys, 10);
const authorSearchApi = new ScopusAuthorSearchApi(client);
const scopusSearchApi = new ScopusSearchApi(client);

let count = 1;

//const idMap = new Map<string, string>();

const idMap = new Map<string, string>([
  ['22988279600', 'Einstein, Albert'],
] as (readonly [string, string])[]);

const [cachingCoauthorStream, previousCoauthorStream] = ReadableStream.from(
  readerToAsyncIterable(await Deno.open(inputFile, { read: true })),
)
  .pipeThrough(new TextDecoderStream())
  .pipeThrough(new TextLineStream())
  .pipeThrough(filter((value) => value.trim() !== ''))
  .pipeThrough(map((value) => value.split('\t').map((term) => term.trim())))
  .pipeThrough(
    map(async ([id, name, ind]) => {
      //idMap.set(id, name);
      const index = `${count++}`.padStart(5, ' ');
      console.info(`start [${index}]:`, id, name, ind);
      // const query = `AU-ID(${id}) AND FIRSTAUTH(${name})`;
      const query = `AU-ID(${id})`;
      console.info(`\t[${index}] loading: ${query}`);
      try {
        if (id === '') {
          throw new Error(`The scopus-id for "${name}" is empty`);
        }

        let isCached = true;

        const body = await (async () => {
          try {
            return JSON.parse(
              await Deno.readTextFile(getCache(id)),
            ) as ScopusSearchResponseBody<ScopusAuthorSearchEntry>;
          } catch {
            isCached = false;
            return await authorSearchApi.search(
              {
                'co-author': id,
                view: 'STANDARD',
                // sort: sortedBy,
                count: 1,
                field: 'url',
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

        console.info(
          `\t[${index}] loaded`,
          `${body['search-results']['entry'].length}/${body['search-results']['opensearch:totalResults']}`,
        );
        return { index, id, name, ind, isCached, body };
      } catch (err: unknown) {
        console.error(`\t[${index}] error`, err);
        return {
          index,
          id,
          name,
          ind,
          isCached: false,
          body:
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
  )
  .tee();

const coAuthorRegEx = /au\-id\((\d+)\)/gi;

const coauthorStream = previousCoauthorStream
  .pipeThrough(
    filter(
      (
        data,
      ): data is typeof data & {
        body: ScopusSearchResponseBody<ScopusAuthorSearchEntry>;
      } => !(data.body instanceof Error),
    ),
  )
  .pipeThrough(
    flatMap(({ index, id, name, ind, body }) =>
      ReadableStream.from(
        [
          ...body['search-results']['opensearch:Query'][
            '@searchTerms'
          ].matchAll(coAuthorRegEx),
        ]
          .map((row) => row[1])
          .filter((coId) => coId !== id)
          .map((coId) => ({
            index,
            id,
            name,
            ind,
            coId,
          })),
      ),
    ),
  );

const cachingCoauthorPromise = cachingCoauthorStream.pipeTo(
  new WritableStream({
    async write({ index, id, name, isCached, body }) {
      if (isCached && getCache(id) === getOuput(id)) {
        console.info(`\t[${index}] done: cached`);
        return;
      }

      const isError = body instanceof Error;
      const encoder = new TextEncoder();
      const dataArray = encoder.encode(
        JSON.stringify(
          isError
            ? {
                name: body.name,
                message: body.message,
                cause: body.cause,
              }
            : body,
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

function getPairValue(coauthor1: string, coauthor2: string): string {
  if (coauthor1 < coauthor2) {
    return `${coauthor1}-${coauthor2}`;
  } else {
    return `${coauthor2}-${coauthor1}`;
  }
}

const pairCoauthorSet = new Set<string>();

const [cachingPairCoauthorStream, previousPairCoauthorStream] =
  ReadableStream.from(await toArray(coauthorStream))
    .pipeThrough(
      filter(({ id, coId }) => {
        if (idMap.has(coId)) {
          return false;
        }

        const pariValue = getPairValue(id, coId);
        if (pairCoauthorSet.has(pariValue)) {
          return false;
        }

        pairCoauthorSet.add(pariValue);
        return true;
      }),
    )
    .pipeThrough(
      map((data, index) => ({
        ...data,
        index: `${index++}`.padStart(5, ' '),
        pariValue: getPairValue(data.id, data.coId),
      })),
    )
    .pipeThrough(
      map(async ({ index, id, name, ind, coId, pariValue }) => {
        console.info(`start [${index}]:`, id, name, ind);

        const query = `AU-ID(${id}) AND AU-ID(${coId})`;
        console.info(`\t[${index}] loading: ${query}`);
        try {
          let isCached = true;

          const body = await (async () => {
            try {
              return JSON.parse(
                await Deno.readTextFile(getPairFileName(pariValue)),
              ) as ScopusSearchResponseBody<ScopusSearchEntry>;
            } catch {
              isCached = false;
              return await scopusSearchApi.search(
                {
                  query,
                  view: 'STANDARD',
                  // sort: sortedBy,
                  count: 1,
                  field: 'url',
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

          console.info(
            `\t[${index}] loaded`,
            `${body['search-results']['entry'].length}/${body['search-results']['opensearch:totalResults']}`,
          );
          return {
            index,
            id,
            name,
            ind,
            coId,
            pariValue,
            isCached,
            body,
            type: 'result' as const,
          };
        } catch (err: unknown) {
          console.error(`\t[${index}] error`, err);
          return {
            index,
            id,
            name,
            ind,
            coId,
            pariValue,
            isCached: false,
            body:
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
            type: 'error' as const,
          };
        }
      }),
    )
    .tee();

const pairCoauthorStream = previousPairCoauthorStream
  .pipeThrough(
    filter(
      (data): data is Exclude<typeof data, { type: 'error' }> =>
        data.type !== 'error',
    ),
  )
  .pipeThrough(
    map(({ index, id, ind, coId, pariValue, body }) => {
      return {
        index: index,
        'au-id-1': id,
        'au-data-name-1': idMap.get(id),
        'au-id-2': coId,
        'au-data-name-2': idMap.get(coId),
        'pair-value': pariValue,
        ind: ind,
        'co-document-count': body['search-results']['opensearch:totalResults'],
      };
    }),
  );

const cachingPairCoauthorPromise = cachingPairCoauthorStream.pipeTo(
  new WritableStream({
    async write({ index, id, pariValue, isCached, body }) {
      if (isCached && getPairCache(id) === getPairOuput(id)) {
        console.info(`\t[${index}] done: cached`);
        return;
      }

      const isError = body instanceof Error;
      const encoder = new TextEncoder();
      const dataArray = encoder.encode(
        JSON.stringify(
          isError
            ? {
                name: body.name,
                message: body.message,
                cause: body.cause,
              }
            : body,
          undefined,
          2,
        ),
      );

      console.info(`\t[${index}] writing to file`);
      const indexPrefix = `inx${index.replaceAll(' ', '0')}`;
      const fileId = pariValue;
      const prefix = isError ? `error-${indexPrefix}-` : '';
      const fpOut = await Deno.open(getPairOuput(`${prefix}${fileId}`), {
        create: true,
        write: true,
      });
      await fpOut.write(dataArray);
      fpOut.close();
      console.info(`\t[${index}] done`);
    },
  }),
);

const writeResultPromise = (async () => {
  const fp = await Deno.open(`${outputDir}/result.csv`, {
    create: true,
    write: true,
    truncate: true,
  });

  let columns: string[] | null = null;

  await pairCoauthorStream
    .pipeThrough(
      flatMap((entry) => {
        if (columns === null) {
          columns = Object.keys(entry);

          return ReadableStream.from([
            stringify([columns], { headers: false }),
            stringify([entry], { headers: false, columns }),
          ]);
        }

        return ReadableStream.from([
          stringify([entry], { headers: false, columns }),
        ]);
      }),
    )
    .pipeThrough(new TextEncoderStream())
    .pipeTo(fp.writable);
})();

await Promise.all([
  cachingCoauthorPromise,
  cachingPairCoauthorPromise,
  writeResultPromise,
]);
