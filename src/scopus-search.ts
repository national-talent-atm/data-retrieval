import {
  readableStreamFromReader,
  TextLineStream,
} from 'https://deno.land/std@0.199.0/streams/mod.ts';
import { stringify } from 'https://deno.land/std@0.199.0/csv/mod.ts';
import { ScopusClient } from './elsevier-clients/scopus-client.ts';
import { filter, flatMap, map } from './streams.ts';
import { ScopusSearchApi } from './elsevier-apis/scopus-search-api.ts';
import { ScopusSearchResults } from './elsevier-types/scopus-search-types.ts';

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

const configName = 'au-0001-8236';
const limit = 15;
const sortedBy = 'coverDate,-title';

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

const client = new ScopusClient(apiKey, 10);
const searchApi = new ScopusSearchApi(client);

let count = 1;

const [stream1, stream2] = readableStreamFromReader(
  await Deno.open(inputFile, { read: true }),
)
  .pipeThrough(new TextDecoderStream())
  .pipeThrough(new TextLineStream())
  .pipeThrough(filter((value) => value.trim() !== ''))
  .pipeThrough(map((value) => value.split('\t').map((term) => term.trim())))
  .pipeThrough(
    map(async ([id, name, ind]) => {
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

        const searchResults = await (async () => {
          try {
            return JSON.parse(
              await Deno.readTextFile(getCache(id)),
            ) as ScopusSearchResults;
          } catch {
            isCached = false;
            return await searchApi.search(
              {
                query: query,
                view: 'COMPLETE',
                sort: sortedBy,
                count: limit,
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
          `${searchResults['entry'].length}/${searchResults['opensearch:totalResults']}`,
        );
        return { index, id, name, ind, isCached, searchResults };
      } catch (err: unknown) {
        console.error(`\t[${index}] error`, err);
        return {
          index,
          id,
          name,
          ind,
          isCached: false,
          searchResults:
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

const flatStream = stream2
  .pipeThrough(
    filter(
      (
        data,
      ): data is typeof data & {
        searchResults: ScopusSearchResults;
      } => !(data.searchResults instanceof Error),
    ),
  )
  .pipeThrough(
    flatMap(({ index, id, name, ind, searchResults }) =>
      ReadableStream.from(
        searchResults['entry'].map((entry) => ({
          index,
          id,
          name,
          ind,
          entry,
        })),
      ),
    ),
  )
  .pipeThrough(
    map(({ index, id, name, ind, entry }) => {
      const author = entry['author']?.find((author) => author['authid'] === id);

      return {
        index: index,
        'au-id': id,
        'au-data-name': name,
        ind: ind,
        'au-authname': author?.['authname'],
        'au-given-name': author?.['given-name'],
        'au-surname': author?.['surname'],
        'dc:title': entry['dc:title'],
        'first-au-id': entry['author']?.[0]?.['authid'],
        'prism:coverDate': entry['prism:coverDate'],
        'prism:publicationName': entry['prism:publicationName'],
        'prism:volume': entry['prism:volume'],
        'fund-no': entry['fund-no'],
        'fund-acr': entry['fund-acr'],
        'fund-sponsor': entry['fund-sponsor'],
        'dc:description': entry['dc:description'],
      };
    }),
  );

await Promise.all([
  stream1.pipeTo(
    new WritableStream({
      async write({ index, id, name, isCached, searchResults }) {
        if (isCached && getCache(id) === getOuput(id)) {
          console.info(`\t[${index}] done: cached`);
          return;
        }

        const isError = searchResults instanceof Error;
        const encoder = new TextEncoder();
        const dataArray = encoder.encode(
          JSON.stringify(
            isError
              ? {
                  name: searchResults.name,
                  message: searchResults.message,
                  cause: searchResults.cause,
                }
              : searchResults,
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
  ),
  (async () => {
    const fp = await Deno.open(`${outputDir}/result.csv`, {
      create: true,
      write: true,
      truncate: true,
    });

    let columns: string[] | null = null;

    await flatStream
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
  })(),
]);
