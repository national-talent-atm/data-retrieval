/**
 * Base function for talent database.
 */
import { stringify } from 'jsr:@std/csv';
import { TextLineStream } from 'jsr:@std/streams';
import { ScopusAuthorRetrievalApi } from '../elsevier-apis/scopus-author-retrieval-api.ts';
import { ScopusSearchApi } from '../elsevier-apis/scopus-search-api.ts';
import { ScopusClient } from '../elsevier-clients/scopus-client.ts';
import { SciValAuthorApi } from '../sci-val-apis/sci-val-author.ts';
import {
  filter,
  flatMap,
  map,
  multipleTee,
  tupleZipReadableStreams,
} from '../streams.ts';
import { readerToAsyncIterable } from '../utils.ts';
import { ExtractFn } from './extractors/extractor.types.ts';
import {
  FetchedStreamData,
  GenerateFetchedStreamData,
  StreamData,
} from './generator.types.ts';

function writeCacheOrErrorToFile(
  fetchingName: string,
  cacheFileFn: (streamData: StreamData) => string,
  errorFileFn: (streamData: StreamData) => string,
  color: string,
): WritableStream<FetchedStreamData> {
  return new WritableStream<{
    index: number;
    id: string;
    indexText: string;
    rest: string[];
    isCached: boolean;
    body: unknown;
  }>({
    async write(fetchedStreamData) {
      const { id, indexText, isCached, body } = fetchedStreamData;

      if (isCached) {
        console.log(
          `\t%c[${indexText}] [${fetchingName}] 💾 ${fetchingName}(${id}) don't need to be cached`,
          `color: ${color}`,
        );
        return;
      }

      const isError = body instanceof Error;
      const encoder = new TextEncoder();
      const encodedData = encoder.encode(
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

      console.log(
        `\t%c[${indexText}] [${fetchingName}] 💾 writing ${fetchingName}(${id}) to file`,
        `color: ${color}`,
      );
      const fpOut = await Deno.open(
        !isError
          ? cacheFileFn(fetchedStreamData)
          : errorFileFn(fetchedStreamData),
        {
          create: true,
          write: true,
        },
      );
      await fpOut.write(encodedData);
      fpOut.close();
      console.log(
        `\t%c[${indexText}] [${fetchingName}] 💾 done ${fetchingName}(${id})`,
        `color: ${color}`,
      );
    },
  });
}

function fetchFromCacheOrApi<O>(
  inputStream: ReadableStream<StreamData>,
  fetchingName: string,
  cacheFileFn: (streamData: StreamData) => string,
  errorFileFn: (streamData: StreamData) => string,
  color: string,
  fetchFn: (
    streamData: StreamData,
    envData: { messagePrefix: string; color: string },
  ) => Promise<O> | O,
  {
    startMessageFn = <const SD extends StreamData>({ id }: SD) =>
      `loading ${fetchingName}(${id})` as const,
    finishMessageFn = <const SD extends StreamData>({ id }: SD) =>
      `${fetchingName}(${id}) loaded` as const,
    errorMessageFn = <const SD extends StreamData>({ id }: SD) =>
      `${fetchingName}(${id}) error on loading` as const,
  } = {},
): readonly [
  ReadableStream<
    | GenerateFetchedStreamData<'result', O>
    | GenerateFetchedStreamData<'error', Error>
  >,
  Promise<void>,
] {
  const [outputStream, cachingStream] = inputStream
    .pipeThrough(
      map(async (streamData) => {
        const { index, id, indexText, rest } = streamData;

        const messagePrefix = `\t%c[${indexText}] [${fetchingName}]`;

        console.log(
          `\t%c[${indexText}] [${fetchingName}] 🚀 ${startMessageFn(streamData)}`,
          `color: ${color}`,
        );

        try {
          if (id === '') {
            throw new Error(`The id for index: "${index}" is empty`);
          }

          let isCached = true;

          const body = await (async () => {
            try {
              return JSON.parse(
                await Deno.readTextFile(cacheFileFn(streamData)),
              ) as O;
            } catch {
              isCached = false;

              return await fetchFn(streamData, { messagePrefix, color });
            }
          })();

          console.log(
            `\t%c[${indexText}] [${fetchingName}] 🏁 ${finishMessageFn(streamData)}`,
            `color: ${color}`,
          );

          return {
            index,
            id,
            indexText,
            rest,
            isCached,
            body,
            type: 'result' as const,
          };
        } catch (err) {
          console.log(
            `\t%c[${indexText}] [${fetchingName}] ⚠ ${errorMessageFn(streamData)}`,
            `color: ${color}`,
            err,
          );

          return {
            index,
            id,
            indexText,
            rest,
            isCached: false,
            body:
              err instanceof Error
                ? new Error(err.message, {
                    cause: {
                      id,
                      error: err.cause,
                      origin: err,
                    },
                  })
                : new Error(`${err}`, {
                    cause: {
                      id,
                      origin: err,
                    },
                  }),
            type: 'error' as const,
          };
        }
      }),
    )
    .tee();

  return [
    outputStream,
    cachingStream.pipeTo(
      writeCacheOrErrorToFile(fetchingName, cacheFileFn, errorFileFn, color),
    ),
  ] as const;
}

const getErrorFileName = (index: number, fileExt: string) => {
  return `error-index-${`${index}`.padStart(5, '0')}-${fileExt}` as const;
};

const defaultGetAuthorFileName = (id: string) => {
  return `au-id-${id}.json` as const;
};

const defaultGetMetricsFileName = (id: string) => {
  return `metrics-au-id-${id}.json` as const;
};

const defaultGetScopusSearchFileName = (id: string) => {
  return `scopus-search-au-id-${id}.json` as const;
};

const defaultIndexTextFn = (index: number): string =>
  `${index}`.padStart(6, ' ');

/**
 * Generate Scopus Talent DB.
 *
 * @param configName
 * @param targetDir
 * @param apiKeys
 * @param extractFn
 * @param options
 * @returns
 */
export async function generate(
  configName: string,
  targetDir: string,
  apiKeys: readonly string[],
  extractFn: ExtractFn,
  {
    inputFileName = `${configName}.txt`,
    outputSubDir = `output/${configName}`,
    cacheSubDir = `output/${configName}/cache`,
    resultFileName = `${configName}-talent-full-result.csv`,
    indexFn = defaultIndexTextFn,
    getAuthorFileName = defaultGetAuthorFileName,
    getMetricsFileName = defaultGetMetricsFileName,
    getScopusSearchFileName = defaultGetScopusSearchFileName,
  } = {},
): Promise<void> {
  const inputFile = `${targetDir}/${inputFileName}`;
  const outputDir = `${targetDir}/${outputSubDir}`;
  const cacheDir = `${targetDir}/${cacheSubDir}`;
  const resultFile = `${outputDir}/${resultFileName}`;

  const getAuthorErrorFileName = <
    const S extends string,
    const N extends number,
  >(
    id: S,
    index: N,
  ) => {
    return getErrorFileName(
      index,
      getAuthorFileName(id === '' ? 'unknown' : id),
    );
  };

  const getMetricsErrorFileName = <
    const S extends string,
    const N extends number,
  >(
    id: S,
    index: N,
  ) => {
    return getErrorFileName(
      index,
      getMetricsFileName(id === '' ? 'unknown' : id),
    );
  };

  const getScopusSearchErrorFileName = <
    const S extends string,
    const N extends number,
  >(
    id: S,
    index: N,
  ) => {
    return getErrorFileName(
      index,
      getScopusSearchFileName(id === '' ? 'unknown' : id),
    );
  };

  const getAuthorCache = <const SD extends StreamData>({ id }: SD) => {
    return `${cacheDir}/${getAuthorFileName(id)}` as const;
  };

  const getAuthorError = <const SD extends StreamData>({ id, index }: SD) => {
    return `${outputDir}/${getAuthorErrorFileName(id, index)}` as const;
  };

  const getMetricsCache = <const SD extends StreamData>({ id }: SD) => {
    return `${cacheDir}/${getMetricsFileName(`${id}`)}` as const;
  };

  const getMetricsError = <const SD extends StreamData>({ id, index }: SD) => {
    return `${outputDir}/${getMetricsErrorFileName(id, index)}` as const;
  };

  const getScopusSearchCache = <const SD extends StreamData>({ id }: SD) => {
    return `${cacheDir}/${getScopusSearchFileName(`${id}`)}` as const;
  };

  const getScopusSearchError = <const SD extends StreamData>({
    id,
    index,
  }: SD) => {
    return `${outputDir}/${getScopusSearchErrorFileName(id, index)}` as const;
  };

  await Deno.mkdir(outputDir, { recursive: true });
  await Deno.mkdir(cacheDir, { recursive: true });

  const client = new ScopusClient(apiKeys, 10);
  const authorRetrievalApi = new ScopusAuthorRetrievalApi(client);
  const authorMetricsApi = new SciValAuthorApi(client);
  const scopusSearchApi = new ScopusSearchApi(client);

  const inputStream = ReadableStream.from(
    readerToAsyncIterable(await Deno.open(inputFile, { read: true }), {
      closeAfterFinish: true,
    }),
  )
    .pipeThrough(new TextDecoderStream())
    .pipeThrough(new TextLineStream())
    .pipeThrough(map((value) => value.trim()))
    .pipeThrough(filter((value) => value !== ''))
    .pipeThrough(
      (() => {
        let index = 1;

        return map((value) => {
          const [id, ...rest] = value.split('\t').map((term) => term.trim());

          const indexText = indexFn(index++);
          console.log(`%cSTART\t[${indexText}] 🌌 ${id}`, 'font-weight: bold');

          return { index, id, indexText, rest };
        });
      })(),
    );

  const [inputAuthorStream, inputMetricsStream, inputScopusSearchStream] =
    multipleTee(inputStream, 3);

  const [authorStream, auhtorCachingPromise] = fetchFromCacheOrApi(
    inputAuthorStream,
    'author',
    getAuthorCache,
    getAuthorError,
    'blue',
    ({ id }, { messagePrefix, color }) =>
      authorRetrievalApi.authorId(
        id,
        {
          view: 'ENHANCED',
        },
        (limit, remaining, reset, status) =>
          console.log(
            `${messagePrefix} ⏱ rateLimit: ${remaining?.padStart(
              5,
              ' ',
            )}/${limit} reset: ${reset} [${status}]`,
            `color: ${color}`,
          ),
      ),
  );

  const [metricsStream, metricsCachingPromise] = fetchFromCacheOrApi(
    inputMetricsStream,
    'metrics',
    getMetricsCache,
    getMetricsError,
    'crimson',
    ({ id }, { messagePrefix, color }) =>
      authorMetricsApi.metrics(
        [id],
        [
          'AcademicCorporateCollaboration',
          'AcademicCorporateCollaborationImpact',
          'Collaboration',
          'CitationCount',
          'CitationsPerPublication',
          'CollaborationImpact',
          'CitedPublications',
          'FieldWeightedCitationImpact',
          'HIndices',
          'ScholarlyOutput',
          'PublicationsInTopJournalPercentiles',
          'OutputsInTopCitationPercentiles',
        ],
        {
          queries: {
            byYear: false,
            yearRange: '10yrs',
          },
          rateLimitNotify: (limit, remaining, reset, status) =>
            console.log(
              `${messagePrefix} ⏱ rateLimit: ${remaining?.padStart(
                5,
                ' ',
              )}/${limit} reset: ${reset} [${status}]`,
              `color: ${color}`,
            ),
        },
      ),
  );

  const [scopusSearchStream, scopusSearchCachingPromise] = fetchFromCacheOrApi(
    inputScopusSearchStream,
    'scopus-search',
    getScopusSearchCache,
    getScopusSearchError,
    'darkcyan',
    ({ id }, { messagePrefix, color }) =>
      scopusSearchApi.search(
        {
          query: `AU-ID(${id})`,
          view: 'COMPLETE',
          sort: 'coverDate,-title',
        },
        (limit, remaining, reset, status) =>
          console.log(
            `${messagePrefix} ⏱ rateLimit: ${remaining?.padStart(
              5,
              ' ',
            )}/${limit} reset: ${reset} [${status}]`,
            `color: ${color}`,
          ),
      ),
  );

  const combinedStream = tupleZipReadableStreams(
    authorStream,
    metricsStream,
    scopusSearchStream,
  )
    .pipeThrough(
      filter(
        (
          tuple,
        ): tuple is [
          Exclude<(typeof tuple)[0], { type: 'error' }>,
          Exclude<(typeof tuple)[1], { type: 'error' }>,
          Exclude<(typeof tuple)[2], { type: 'error' }>,
        ] => {
          const valid =
            tuple[0].type !== 'error' &&
            tuple[1].type !== 'error' &&
            tuple[2].type !== 'error' &&
            tuple[0].body['author-retrieval-response'].length > 0 &&
            tuple[1].body.results.length > 0 &&
            tuple[2].body['search-results'].entry.length > 0;

          if (!valid) {
            const { id, indexText } = tuple[0];
            console.log(
              `%IGNORE\t[${indexText}] ❌ ${id}, there are some errors`,
              'font-weight: bold; color: red',
            );
          }

          return valid;
        },
      ),
    )
    .pipeThrough(
      map((combinedResult) => {
        const { id, indexText } = combinedResult[0];

        const result = extractFn(combinedResult);

        console.log(
          `%cFINISH\t%c[${indexText}] 🏁 ${id}`,
          'font-weight: bold; text-decoration: underline',
          'text-decoration: none',
        );

        return result;
      }),
    );

  const resultPromise = (async () => {
    const fp = await Deno.open(resultFile, {
      create: true,
      write: true,
      truncate: true,
    });

    await fp.write(new Uint8Array([0xef, 0xbb, 0xbf]));

    let columns: string[] | null = null;

    return await combinedStream
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
    auhtorCachingPromise,
    metricsCachingPromise,
    scopusSearchCachingPromise,
    resultPromise,
  ]);

  return;
}
