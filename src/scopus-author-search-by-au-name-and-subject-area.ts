import { TextLineStream } from 'jsr:@std/streams';
import { stringify } from 'jsr:@std/csv';
import { ScopusAuthorSearchApi } from './elsevier-apis/scopus-author-search-api.ts';
import { ScopusClient } from './elsevier-clients/scopus-client.ts';
import { readerToAsyncIterable } from './utils.ts';
import { filter, flatMap, map } from './streams.ts';
import { ScopusSearchResponseBody } from './elsevier-types/scopus-types.ts';
import { ScopusAuthorSearchEntry } from './elsevier-types/scopus-author-search-types.ts';

const getFileName = (name: string) => {
  const fileId = name.replaceAll(' ', '_');
  return `au-name-${fileId}.json` as const;
};

const apiKey = Deno.env.get('ELSEVIER_KEY');

if (!apiKey) {
  console.error(
    `API key must be specified by environment variable 'ELSEVIER_KEY'`,
  );
  Deno.exit(-1);
}

const apiKeys = apiKey.split(/\s*,\s*/gi).filter((value) => value !== '');

const configName = 'top200-mahidol-university-medicine-20241205';

const inputFile = `./target/${configName}.txt` as const;
const outputDir = `./target/output/${configName}` as const;
const catchDir = outputDir;
const resultFile = `${outputDir}/${configName}-result.csv`;

const getCache = (name: string) => {
  return `${catchDir}/${getFileName(name)}` as const;
};

const getOuput = (name: string) => {
  return `${outputDir}/${getFileName(name)}` as const;
};

await Deno.mkdir(outputDir, { recursive: true });

const client = new ScopusClient(apiKeys, 10);
const authorSearchApi = new ScopusAuthorSearchApi(client);

const inputStream = ReadableStream.from(
  readerToAsyncIterable(await Deno.open(inputFile, { read: true })),
)
  .pipeThrough(new TextDecoderStream())
  .pipeThrough(new TextLineStream())
  .pipeThrough(map((value) => value.trim()))
  .pipeThrough(filter((value) => value !== ''))
  .pipeThrough(
    (() => {
      let count = 1;

      return map((value) => {
        const [auName, subjectArea, ind, nPub] = value
          .split('\t')
          .map((term) => term.trim());

        const [lastName, firstName] = auName
          .split(',', 2)
          .map((value) => value.trim());

        const index = `${count++}`.padStart(5, ' ');
        console.info(`start [${index}]:`, firstName, lastName);

        return { lastName, firstName, subjectArea, ind, nPub, auName, index };
      });
    })(),
  );

const authorStream = inputStream.pipeThrough(
  map(
    async ({ lastName, firstName, subjectArea, ind, nPub, auName, index }) => {
      const query = `AUTHLASTNAME(${lastName}) AND AUTHFIRST(${firstName}) AND SUBJAREA(${subjectArea})`;
      console.info(`\t[${index}] loading author: ${query}`);

      try {
        if (auName === '') {
          throw new Error(`The auName is empty`);
        }

        let isCached = true;

        const body = await (async () => {
          try {
            return JSON.parse(
              await Deno.readTextFile(getCache(auName)),
            ) as ScopusSearchResponseBody<ScopusAuthorSearchEntry>;
          } catch {
            isCached = false;
            return await authorSearchApi.search(
              {
                query,
                view: 'STANDARD',
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
        return {
          index,
          lastName,
          firstName,
          subjectArea,
          ind,
          nPub,
          auName,
          isCached,
          body,
          type: 'result' as const,
        };
      } catch (err) {
        console.error(`\t[${index}] error`, err);
        return {
          index,
          lastName,
          firstName,
          subjectArea,
          ind,
          nPub,
          auName,
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
    },
  ),
);

const [cachingAuthorStream, preResultsAuthorStream] = authorStream.tee();

const cachingAuthorPromise = cachingAuthorStream.pipeTo(
  new WritableStream({
    async write({ index, auName, isCached, body }) {
      if (isCached && getCache(auName) === getOuput(auName)) {
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
      const fileId = auName === '' ? `${indexPrefix}` : auName;
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

const resultsAuthorStream = preResultsAuthorStream
  .pipeThrough(
    filter((result): result is Exclude<typeof result, { type: 'error' }> => {
      return (
        result.type !== 'error' &&
        +result.body['search-results']['opensearch:totalResults'] > 0
      );
    }),
  )
  .pipeThrough(
    flatMap(
      ({
        index,
        lastName,
        firstName,
        subjectArea,
        ind,
        nPub,
        auName,
        body,
      }) => {
        const sortedEntries = body['search-results']['entry'].sort(
          (pre, next) => {
            const docCountPre = +pre['document-count'];
            const docCountNext = +next['document-count'];

            return docCountPre > docCountNext
              ? -1
              : docCountPre < docCountNext
              ? 1
              : 0;
          },
        );

        const result =
          sortedEntries.find(
            (entry) =>
              entry['preferred-name'].surname === lastName &&
              entry['preferred-name']['initials'] === firstName,
          ) ?? sortedEntries[0];

        return ReadableStream.from(
          result
            ? (() => {
                const id = result['dc:identifier'].split(':')[1];
                const preferredName = result['preferred-name'];
                const document_count = result['document-count'];

                return [
                  {
                    id,
                    auName,
                    ind,
                    'given-name': preferredName['given-name'],
                    surname: preferredName['surname'],
                    initials: preferredName['initials'],
                    document_count,
                    lastName,
                    firstName,
                    subjectArea,
                    nPub,
                    index,
                  },
                ];
              })()
            : [],
        );
      },
    ),
  );

const resultsAuthorPromise = (async () => {
  const fp = await Deno.open(resultFile, {
    create: true,
    write: true,
    truncate: true,
  });

  let columns: string[] | null = null;

  await resultsAuthorStream
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

await Promise.all([cachingAuthorPromise, resultsAuthorPromise]);
