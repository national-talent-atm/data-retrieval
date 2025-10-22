import { parseArgs } from 'jsr:@std/cli@1.0.23';
import { stringify } from 'jsr:@std/csv@1.0.6';
import { TextLineStream } from 'jsr:@std/streams@1.0.13';
import { ScopusAuthorSearchApi } from '../../elsevier-apis/scopus-author-search-api.ts';
import { ScopusClient } from '../../elsevier-clients/scopus-client.ts';
import { ScopusAuthorSearchEntry } from '../../elsevier-types/scopus-author-search-types.ts';
import { ScopusSearchResponseBody } from '../../elsevier-types/scopus-types.ts';
import { filter, flatMap, map } from '../../streams.ts';

const getFileName = (name: string) => {
  const fileId = name.replaceAll(' ', '_');
  return `full-name-${fileId}.json` as const;
};

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

const targetDir =
  (!parsedArgs['target'] ? undefined : parsedArgs['target']) ?? './target';

const inputFile = `${targetDir}/${configName}.txt` as const;
const outputDir = `${targetDir}/output/${configName}` as const;
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

const inputStream = (await Deno.open(inputFile, { read: true })).readable
  .pipeThrough(new TextDecoderStream())
  .pipeThrough(new TextLineStream())
  .pipeThrough(map((value) => value.trim()))
  .pipeThrough(filter((value) => value !== ''))
  .pipeThrough(
    (() => {
      let count = 1;

      return map((value) => {
        const [x_nameSearh, x_firstNameEn, x_lastNameEn, x_industry] = value
          .split('\t')
          .map((term) => term.trim());

        const index = `${count++}`.padStart(5, ' ');
        const name = `${index.replaceAll(' ', '0')}-${x_firstNameEn}-${x_lastNameEn}`;
        const names = x_nameSearh.split(';').map((fullname) => {
          const [firstName, lastName] = fullname
            .split(' ', 2)
            .map((nameSearh) => nameSearh.replaceAll('_', ' '));
          return { firstName, lastName };
        });
        console.info(`start [${index}]:`, x_nameSearh);

        return {
          names,
          name,
          index,
          xs: {
            x_nameSearh,
            x_firstNameEn,
            x_lastNameEn,
            x_industry,
          },
        };
      });
    })(),
  );

const authorStream = inputStream.pipeThrough(
  map(async ({ names, name, index, xs }) => {
    const query = names
      .map(
        ({ firstName, lastName }) =>
          `(AUTHFIRST(${firstName}) AND AUTHLASTNAME(${lastName}))`,
      )
      .join(' OR ');
    console.info(`\t[${index}] loading author: ${query}`);

    try {
      if (query === '') {
        throw new Error(`The names is empty`);
      }

      let isCached = true;

      const body = await (async () => {
        try {
          return JSON.parse(
            await Deno.readTextFile(getCache(name)),
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
        names,
        name,
        xs,
        isCached,
        body,
        type: 'result' as const,
      };
    } catch (err) {
      console.error(`\t[${index}] error`, err);
      return {
        index,
        names,
        name,
        xs,
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

const [cachingAuthorStream, preResultsAuthorStream] = authorStream.tee();

const cachingAuthorPromise = cachingAuthorStream.pipeTo(
  new WritableStream({
    async write({ index, name, isCached, body }) {
      if (isCached && getCache(name) === getOuput(name)) {
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
      const fileId = name === '' ? `${indexPrefix}` : name;
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

const resultsAuthorStream = preResultsAuthorStream.pipeThrough(
  flatMap((result) =>
    ReadableStream.from(
      ((result): result is Exclude<typeof result, { type: 'error' }> =>
        result.type !== 'error' &&
        +result.body['search-results']['opensearch:totalResults'] > 0)(result)
        ? (({ index, body, xs }) =>
            body['search-results']['entry'].map((result) => {
              const id = result['dc:identifier'].split(':')[1];
              const preferredName = result['preferred-name'];
              const documentCount = result['document-count'];

              return {
                id,
                index,
                surname: preferredName['surname'],
                'given-name': preferredName['given-name'],
                initials: preferredName['initials'],
                'document-count': documentCount,
                ...xs,
              };
            }))(result)
        : (({ index, xs }) => [
            {
              id: '[not-found]',
              index,
              surname: '',
              'given-name': '',
              initials: '',
              'document-count': '',
              ...xs,
            },
          ])(result),
    ),
  ),
);

const resultsAuthorPromise = (async () => {
  const fp = await Deno.open(resultFile, {
    create: true,
    write: true,
    truncate: true,
  });

  await fp.write(new Uint8Array([0xef, 0xbb, 0xbf]));

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
