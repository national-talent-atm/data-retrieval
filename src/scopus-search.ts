import {
  readableStreamFromIterable,
  readableStreamFromReader,
  TextLineStream,
} from 'https://deno.land/std@0.186.0/streams/mod.ts';
// @deno-types="https://deno.land/x/sheetjs@v0.18.3/types/index.d.ts"
import {
  utils as xlsxUtils,
  write as xlsxWrite,
} from 'https://deno.land/x/sheetjs@v0.18.3/xlsx.mjs';
import { ScopusClient } from './elsevier-clients/scopus-client.ts';
import { filter, flatMap, map } from './streams.ts';
import { ScopusSearchApi } from './elsevier-apis/scopus-search-api.ts';
import { ScopusSearchResults } from './elsevier-types/scopus-search-types.ts';

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

const inputFile = `./target/${configName}.txt`;
const outputDir = `./target/output/${configName}`;

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
      console.info('\tloading');
      // const query = `AU-ID(${id}) AND FIRSTAUTH(${name})`;
      const query = `AU-ID(${id})`;
      try {
        if (id === '') {
          throw new Error(`The scopus-id for "${name}" is empty`);
        }

        const searchResults = await searchApi.search({
          query: query,
          view: 'COMPLETE',
          sort: sortedBy,
          count: limit,
        });

        console.info(
          '\tloaded',
          `${searchResults['entry'].length}/${searchResults['opensearch:totalResults']}`,
        );
        return { id, name, ind, searchResults };
      } catch (err: unknown) {
        console.error(`\terror`, err);
        return {
          id,
          name,
          ind,
          searchResults: new Error(
            err instanceof Error ? err.message : `${err}`,
            {
              cause: {
                query: query,
                origin: err,
              },
            },
          ),
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
    flatMap(({ id, name, ind, searchResults }) =>
      readableStreamFromIterable(
        searchResults['entry'].map((entry) => ({ id, name, ind, entry })),
      ),
    ),
  )
  .pipeThrough(
    map(({ id, name, ind, entry }) => {
      const author = entry['author']?.find((author) => author['authid'] === id);

      return {
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
      async write({ id, name, searchResults }) {
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

        console.info('\twriting to file');
        const suffix = isError
          ? `-${Math.floor(Math.random() * 10000)}-error`
          : '';
        const fpOut = await Deno.open(
          `${outputDir}/au-id-${isError ? name : id}${suffix}.json`,
          {
            create: true,
            write: true,
          },
        );
        await fpOut.write(dataArray);
        fpOut.close();
        console.info('\tdone');
      },
    }),
  ),
  (async () => {
    const results: unknown[] = [];

    await flatStream.pipeTo(
      new WritableStream({
        write(entry) {
          results.push(entry);
        },
      }),
    );

    const fp = await Deno.open(`${outputDir}/result.xlsx`, {
      create: true,
      write: true,
      truncate: true,
    });

    const wb = xlsxUtils.book_new();
    xlsxUtils.book_append_sheet(wb, xlsxUtils.json_to_sheet(results));
    const buff = xlsxWrite(wb, { type: 'array', bookType: 'xlsx' });
    await fp.write(buff);
  })(),
]);
