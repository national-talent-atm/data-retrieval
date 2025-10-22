import { stringify } from 'jsr:@std/csv@1.0.6';
import { TextLineStream } from 'jsr:@std/streams@1.0.13';
import { ScopusSearchApi } from './elsevier-apis/scopus-search-api.ts';
import { ScopusClient } from './elsevier-clients/scopus-client.ts';
import { ScopusSearchEntry } from './elsevier-types/scopus-search-types.ts';
import { ScopusSearchResponseBody } from './elsevier-types/scopus-types.ts';
import { filter, flatMap, map } from './streams.ts';

const getFileName = (fileId: string) => {
  return `scopus-search-au-id-${fileId}.json` as const;
};

const apiKey = Deno.env.get('ELSEVIER_KEY');

if (!apiKey) {
  console.error(
    `API key must be specified by environment variable 'ELSEVIER_KEY'`,
  );
  Deno.exit(-1);
}

const apiKeys = apiKey.split(/\s*,\s*/gi).filter((value) => value !== '');

const configName = 'yount-talent-with-id-20240110';
const limit = 15;
const sortedBy = 'coverDate,-title';

const inputFile = `./target/${configName}.txt` as const;
const outputDir = `./target/output/${configName}` as const;
const catchDir = outputDir;
const resultFile = `${outputDir}/${configName}-scopus-search-result.csv`;

const getCache = (fileId: string) => {
  return `${catchDir}/${getFileName(`${fileId}`)}` as const;
};

const getOuput = (fileId: string) => {
  return `${outputDir}/${getFileName(`${fileId}`)}` as const;
};

await Deno.mkdir(outputDir, { recursive: true });

const client = new ScopusClient(apiKeys, 10);
const scopusSearchApi = new ScopusSearchApi(client);

const inputStream = (await Deno.open(inputFile, { read: true })).readable
  .pipeThrough(new TextDecoderStream())
  .pipeThrough(new TextLineStream())
  .pipeThrough(map((value) => value.trim()))
  .pipeThrough(filter((value) => value !== ''))
  .pipeThrough(
    (() => {
      let count = 1;

      return map((value) => {
        const [id, name, ind] = value.split('\t').map((term) => term.trim());

        const index = `${count++}`.padStart(5, ' ');
        console.info(`start [${index}]:`, id, name, ind);

        return { id, name, ind, index };
      });
    })(),
  );

const dataStream = inputStream.pipeThrough(
  map(async ({ id, name, ind, index }) => {
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
          ) as ScopusSearchResponseBody<ScopusSearchEntry>;
        } catch {
          isCached = false;
          return await scopusSearchApi.search(
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
        `${body['search-results']['entry'].length}/${body['search-results']['opensearch:totalResults']}`,
      );
      return { index, id, name, ind, isCached, body, type: 'result' as const };
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
        type: 'error' as const,
      };
    }
  }),
);

const [preScopusSearchStream, cachingDataStream] = dataStream.tee();

const cachingDataPromise = cachingDataStream.pipeTo(
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

const scopusSearchStream = preScopusSearchStream
  .pipeThrough(
    filter(
      (data): data is Exclude<typeof data, { type: 'error' }> =>
        data.type !== 'error',
    ),
  )
  .pipeThrough(
    flatMap(({ index, id, name, ind, body }) =>
      ReadableStream.from(
        body['search-results']['entry'].map((entry) => ({
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

const scopusSearchPromise = (async () => {
  const fp = await Deno.open(resultFile, {
    create: true,
    write: true,
    truncate: true,
  });

  let columns: string[] | null = null;

  return await scopusSearchStream
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

await Promise.all([cachingDataPromise, scopusSearchPromise]);
