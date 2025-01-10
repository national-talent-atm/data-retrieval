/**
 * Base function for talent database.
 */
import { stringify } from 'jsr:@std/csv';
import { TextLineStream } from 'jsr:@std/streams';
import asjc from './asjc.json' with { type: 'json' };
import { ScopusAuthorRetrievalApi } from './elsevier-apis/scopus-author-retrieval-api.ts';
import { ScopusSearchApi } from './elsevier-apis/scopus-search-api.ts';
import { ScopusClient } from './elsevier-clients/scopus-client.ts';
import {
  AuthorMetricsResponseBody,
  MetricResult,
} from './elsevier-types/sci-val-author-types.ts';
import { ScopusAuthorResponseBody } from './elsevier-types/scopus-author-types.ts';
import {
  ScopusSearchAuthor,
  ScopusSearchEntry,
} from './elsevier-types/scopus-search-types.ts';
import {
  ScopusName,
  ScopusSearchResponseBody,
} from './elsevier-types/scopus-types.ts';
import { SciValAuthorApi } from './sci-val-apis/sci-val-author.ts';
import {
  filter,
  flatMap,
  map,
  multipleTee,
  tupleZipReadableStreams,
} from './streams.ts';
import { readerToAsyncIterable } from './utils.ts';

type xxx<A> = A extends never ? [A, 1] : [1, A];

type y = xxx<'a' | 'b'>;

type AsjcCode = keyof typeof asjc;

type StreamData = {
  index: number;
  id: string;
  indexText: string;
  rest: string[];
};

type GenerateFetchedStreamData<
  T extends string,
  BT = unknown,
> = T extends unknown
  ? StreamData & {
      isCached: boolean;
      body: BT;
      type: T;
    }
  : never;

type FetchedStreamData<BT = unknown> = GenerateFetchedStreamData<
  'result' | 'error',
  BT
>;

function writeCacheOrErrorToFile(
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
      const { indexText, isCached, body } = fetchedStreamData;

      if (isCached) {
        console.log(`\t%c[${indexText}] 💾 done: cached`, `color: ${color}`);
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

      console.log(`\t%c[${indexText}] 💾 writing to file`, `color: ${color}`);
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
      console.log(`\t%c[${indexText}] 💾 done`, `color: ${color}`);
    },
  });
}

function fetchFromCacheOrApi<O>(
  inputStream: ReadableStream<StreamData>,
  fetchingName: string,
  cacheFileFn: (streamData: StreamData) => string,
  errorFileFn: (streamData: StreamData) => string,
  color: string,
  fetchFn: (streamData: StreamData) => Promise<O> | O,
  {
    startMessageFn = <const SD extends StreamData>({ id }: SD) =>
      `loading ${fetchingName}: ${id}` as const,
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

        console.log(
          `\t%c[${indexText}] ${startMessageFn(streamData)}`,
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

              return await fetchFn(streamData);
            }
          })();

          console.log(
            `\t%c[${indexText}] ${fetchingName} loaded`,
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
            `\t%c[${indexText}] ⚠ ${fetchingName} error on loading`,
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
      writeCacheOrErrorToFile(cacheFileFn, errorFileFn, color),
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

export type AdditionalDataFn = (args: {
  index: number;
  id: string;
  rest: string[];
  scopusAuthorResponseBody: ScopusAuthorResponseBody;
  authorMetricsResponseBody: AuthorMetricsResponseBody;
  scopusSearchResponseBody: ScopusSearchResponseBody<ScopusSearchEntry>;
}) => { [key: string]: unknown };

const defaultIndexTextFn = (index: number): string =>
  `${index}`.padStart(5, ' ');

export async function generate(
  configName: string,
  targetDir: string,
  apiKeys: readonly string[],
  {
    inputFileName = `${configName}.txt`,
    outputSubDir = `output/${configName}`,
    cacheSubDir = `output/${configName}/cache`,
    resultFileName = `${configName}-talent-full-result.csv`,
    additionalDataFn = undefined as AdditionalDataFn | undefined,
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
    readerToAsyncIterable(await Deno.open(inputFile, { read: true })),
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
          console.log(`start [${indexText}]:`, id);

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
    ({ id, indexText }) =>
      authorRetrievalApi.authorId(
        id,
        {
          view: 'ENHANCED',
        },
        (limit, remaining, reset, status) =>
          console.log(
            `\t%c[${indexText}] rateLimit: ${remaining?.padStart(
              5,
              ' ',
            )}/${limit} reset: ${reset} [${status}]`,
            'color: blue',
          ),
      ),
  );

  // const [authorStream, auhtorCachingStream] = inputAuthorStream
  //   .pipeThrough(
  //     map(async ({ index, id, indexText, rest }) => {
  //       const query = `AU-ID(${id})`;
  //       console.log(
  //         `\t%c[${indexText}] loading author: ${query}`,
  //         'color: blue',
  //       );

  //       try {
  //         if (id === '') {
  //           throw new Error(`The scopus-id for index: "${index}" is empty`);
  //         }

  //         let isCached = true;

  //         const body = await (async () => {
  //           try {
  //             return JSON.parse(
  //               await Deno.readTextFile(getAuthorCache(id)),
  //             ) as ScopusAuthorResponseBody;
  //           } catch {
  //             isCached = false;
  //             return await authorRetrievalApi.authorId(
  //               id,
  //               {
  //                 view: 'ENHANCED',
  //               },
  //               (limit, remaining, reset, status) =>
  //                 console.log(
  //                   `\t%c[${indexText}] rateLimit: ${remaining?.padStart(
  //                     5,
  //                     ' ',
  //                   )}/${limit} reset: ${reset} [${status}]`,
  //                   'color: blue',
  //                 ),
  //             );
  //           }
  //         })();

  //         console.log(`\t%c[${indexText}] loaded`, 'color: blue');

  //         return {
  //           index,
  //           id,
  //           indexText,
  //           rest,
  //           isCached,
  //           body,
  //           type: 'result' as const,
  //         };
  //       } catch (err) {
  //         console.error(`\t[${indexText}] error`, err);

  //         return {
  //           index,
  //           id,
  //           indexText,
  //           rest,
  //           isCached: false,
  //           body:
  //             err instanceof Error
  //               ? new Error(err.message, {
  //                   cause: {
  //                     query: query,
  //                     error: err.cause,
  //                     origin: err,
  //                   },
  //                 })
  //               : new Error(`${err}`, {
  //                   cause: {
  //                     query: query,
  //                     origin: err,
  //                   },
  //                 }),
  //           type: 'error' as const,
  //         };
  //       }
  //     }),
  //   )
  //   .tee();

  // const auhtorCachingPromise = auhtorCachingStream.pipeTo(
  //   writeCacheOrErrorToFile(getAuthorCache, getAuthorError, 'blue'),
  // );

  const [metricsStream, metricsCachingStream] = inputMetricsStream
    .pipeThrough(
      map(async (streamData) => {
        const { index, id, indexText, rest } = streamData;

        const query = `AU-ID(${id})`;
        console.log(
          `\t%c[${indexText}] loading metrics: ${query}`,
          'color: crimson',
        );

        try {
          if (id === '') {
            throw new Error(`The scopus-id for index: "${index}" is empty`);
          }

          let isCached = true;

          const body = await (async () => {
            try {
              return JSON.parse(
                await Deno.readTextFile(getMetricsCache(streamData)),
              ) as AuthorMetricsResponseBody;
            } catch {
              isCached = false;
              return await authorMetricsApi.metrics(
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
                      `\t%c[${indexText}] rateLimit: ${remaining?.padStart(
                        5,
                        ' ',
                      )}/${limit} reset: ${reset} [${status}]`,
                      'color: crimson',
                    ),
                },
              );
            }
          })();

          console.log(`\t%c[${indexText}] loaded`, 'color: crimson');

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
          console.error(`\t[${indexText}] error`, err);

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

  const metricsCachingPromise = metricsCachingStream.pipeTo(
    writeCacheOrErrorToFile(getMetricsCache, getMetricsError, 'crimson'),
  );

  const sortedBy = 'coverDate,-title';

  const [scopusSearchStream, scopusSearchCachingStream] =
    inputScopusSearchStream
      .pipeThrough(
        map(async (streamData) => {
          const { index, id, indexText, rest } = streamData;

          // const query = `AU-ID(${id}) AND FIRSTAUTH(${name})`;
          const query = `AU-ID(${id})`;
          console.log(
            `\t%c[${indexText}] loading: ${query}`,
            'color: darkcyan',
          );
          try {
            if (id === '') {
              throw new Error(`The scopus-id for index: "${index}" is empty`);
            }

            let isCached = true;

            const body = await (async () => {
              try {
                return JSON.parse(
                  await Deno.readTextFile(getScopusSearchCache(streamData)),
                ) as ScopusSearchResponseBody<ScopusSearchEntry>;
              } catch {
                isCached = false;
                return await scopusSearchApi.search(
                  {
                    query: query,
                    view: 'COMPLETE',
                    sort: sortedBy,
                  },
                  (limit, remaining, reset, status) =>
                    console.log(
                      `\t%c[${indexText}] rateLimit: ${remaining?.padStart(
                        5,
                        ' ',
                      )}/${limit} reset: ${reset} [${status}]`,
                      'color: darkcyan',
                    ),
                );
              }
            })();

            console.log(
              `\t%c[${indexText}] loaded: ${body['search-results']['entry'].length}/${body['search-results']['opensearch:totalResults']}`,
              'color: darkcyan',
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
          } catch (err: unknown) {
            console.error(`\t[${indexText}] error`, err);

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

  const scopusSearchCachingPromise = scopusSearchCachingStream.pipeTo(
    writeCacheOrErrorToFile(
      getScopusSearchCache,
      getScopusSearchError,
      'darkcyan',
    ),
  );

  function extractMatric<MT extends MetricResult['metricType']>(
    metrics: readonly MetricResult[] | MetricResult[],
    type: MT,
  ): Extract<MetricResult, { metricType: MT }> | null {
    for (const metric of metrics) {
      if (metric.metricType === type) {
        return metric as Extract<MetricResult, { meticType: MT }>;
      }
    }

    return null;
  }

  function extractName(author: ScopusSearchAuthor | ScopusName): string;
  function extractName(author: undefined): undefined;
  function extractName(
    author: ScopusSearchAuthor | ScopusName | undefined,
  ): string | undefined {
    if (!author) {
      return undefined;
    }

    return `${author['surname']}, ${author['given-name']}`;
  }

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
          return (
            tuple[0].type !== 'error' &&
            tuple[1].type !== 'error' &&
            tuple[2].type !== 'error' &&
            tuple[0].body['author-retrieval-response'].length > 0 &&
            tuple[1].body.results.length > 0 &&
            tuple[2].body['search-results'].entry.length > 0
          );
        },
      ),
    )
    .pipeThrough(
      map(([authorBody, metricsBody, scopusSearchBody]) => {
        const { index, id, indexText, rest } = authorBody;
        console.log(
          `\t%c[${indexText}] combinding: AUTHOR:${authorBody.id} | METRICS:${metricsBody.id} | SCOPUS-SEARCH:${scopusSearchBody.id}`,
          'color: darkorange',
        );
        if (
          authorBody.id !== metricsBody.id ||
          authorBody.id !== scopusSearchBody.id
        ) {
          throw 'ID miss match';
        }

        const authorResult = authorBody.body['author-retrieval-response'][0];
        const metrics = metricsBody.body.results[0].metrics;
        const scopusSearchResults =
          scopusSearchBody.body['search-results'].entry;

        const asjcSorted = ((classifications) =>
          classifications['@type'] === 'ASJC'
            ? Array.isArray(classifications['classification'])
              ? classifications['classification']
              : [classifications['classification']]
            : [])(
          authorResult['author-profile']['classificationgroup'][
            'classifications'
          ],
        ).sort((pre, next) => {
          const feqPre = +pre['@frequency'];
          const feqNext = +next['@frequency'];

          return feqPre > feqNext ? -1 : feqPre < feqNext ? 1 : 0;
        });

        const asjcCode = asjcSorted[0]?.$ ?? '0000';
        const asjcFeq = asjcSorted[0]?.['@frequency'] ?? 'Unknown';

        const asjcData = asjc[asjcCode as AsjcCode] ?? {
          'subject-area': 'Unknown',
          'research-branch': 'Unknown',
          'research-field': 'Unknown',
        };

        const keywordMap = new Map<string, number>();

        for (const scopusSearchResult of scopusSearchResults) {
          (scopusSearchResult.authkeywords ?? '')
            .split(/\s*\|\s*/)
            .filter((keyword) => keyword.trim() !== '')
            .forEach((keyword) => {
              keywordMap.set(keyword, (keywordMap.get(keyword) ?? 0) + 1);
            });
        }

        const sortedKeywordEntries = [...keywordMap.entries()].sort(
          (pre, next) => (pre[1] > next[1] ? -1 : pre[1] < next[1] ? 1 : 0),
        );

        const coauthorMap = new Map<
          string,
          { count: number; author: ScopusSearchAuthor }
        >();

        for (const scopusSearchResult of scopusSearchResults) {
          (scopusSearchResult.author ?? []).forEach((author) => {
            if (author.authid !== authorBody.id) {
              if (coauthorMap.has(author.authid)) {
                const preData = coauthorMap.get(author.authid)!;
                preData.count++;
                coauthorMap.set(author.authid, preData);
              } else {
                coauthorMap.set(author.authid, { count: 1, author: author });
              }
            }
          });
        }

        const sortedCoauthorEntries = [...coauthorMap.entries()].sort(
          (pre, next) =>
            pre[1].count > next[1].count
              ? -1
              : pre[1].count < next[1].count
                ? 1
                : 0,
        );

        const givenName =
          authorResult['author-profile']['preferred-name']['given-name'];
        const surname =
          authorResult['author-profile']['preferred-name'].surname;

        const ipDoc = ((currentAffiliation) =>
          (Array.isArray(currentAffiliation)
            ? (currentAffiliation.find(
                (affiliation) => affiliation['ip-doc']['@type'] === 'dept',
              ) ?? currentAffiliation[0])
            : currentAffiliation)['ip-doc'])(
          authorResult['author-profile']['affiliation-current'].affiliation,
        );

        const aff = ((ipDoc) => {
          const { department, facAndUni } =
            ipDoc['@type'] === 'dept'
              ? {
                  department: ipDoc['preferred-name']?.$,
                  facAndUni: ipDoc['parent-preferred-name']?.$ ?? '',
                }
              : { facAndUni: ipDoc['preferred-name']?.$ ?? '' };
          const { faculty, university } = ((splitFacAndUni) =>
            splitFacAndUni.length === 2
              ? {
                  faculty: splitFacAndUni[0],
                  university: splitFacAndUni[1],
                }
              : {
                  university: splitFacAndUni[0],
                })(facAndUni.split(/\s*,(?=[^,]*$)\s*/u));

          return {
            department,
            faculty,
            university,
            city: ipDoc.address?.city,
            country: ipDoc.address?.country,
          };
        })(ipDoc);

        return {
          id,
          ...(typeof additionalDataFn === 'undefined'
            ? {}
            : additionalDataFn({
                index,
                id,
                rest,
                scopusAuthorResponseBody: authorBody.body,
                authorMetricsResponseBody: metricsBody.body,
                scopusSearchResponseBody: scopusSearchBody.body,
              })),
          'given-name': givenName,
          surname,
          name: authorBody.body['author-retrieval-response'][0][
            'author-profile'
          ]['preferred-name']['indexed-name'],
          asjc: asjcCode,
          'asjc-frequency': asjcFeq,
          ...asjcData,
          keyword1: sortedKeywordEntries[0]?.[0],
          keyword2: sortedKeywordEntries[1]?.[0],
          keyword3: sortedKeywordEntries[2]?.[0],
          keyword4: sortedKeywordEntries[3]?.[0],
          keyword5: sortedKeywordEntries[4]?.[0],

          fwci: extractMatric(metrics, 'FieldWeightedCitationImpact')?.value,
          h_index: authorResult['h-index'],
          scholarly_output: extractMatric(metrics, 'ScholarlyOutput')?.value,
          most_recent_pub:
            authorResult['author-profile']?.['publication-range']?.['@end'],
          citation: extractMatric(metrics, 'CitationCount')?.value,
          citation_per_pub: extractMatric(metrics, 'CitationsPerPublication')
            ?.value,
          citation_count: authorResult.coredata?.['citation-count'],
          cited_by_count: authorResult.coredata?.['cited-by-count'],
          document_count: authorResult.coredata?.['document-count'],

          no_of_coauthor: authorResult['coauthor-count'],
          co_author1: extractName(sortedCoauthorEntries[0]?.[1]?.author),
          co_author1_id: sortedCoauthorEntries[0]?.[1]?.author?.authid,
          co_author2: extractName(sortedCoauthorEntries[1]?.[1]?.author),
          co_author2_id: sortedCoauthorEntries[1]?.[1]?.author?.authid,
          co_author3: extractName(sortedCoauthorEntries[2]?.[1]?.author),
          co_author3_id: sortedCoauthorEntries[2]?.[1]?.author?.authid,
          co_author4: extractName(sortedCoauthorEntries[3]?.[1]?.author),
          co_author4_id: sortedCoauthorEntries[3]?.[1]?.author?.authid,
          co_author5: extractName(sortedCoauthorEntries[4]?.[1]?.author),
          co_author5_id: sortedCoauthorEntries[4]?.[1]?.author?.authid,
          co_author6: extractName(sortedCoauthorEntries[5]?.[1]?.author),
          co_author6_id: sortedCoauthorEntries[5]?.[1]?.author?.authid,
          co_author7: extractName(sortedCoauthorEntries[6]?.[1]?.author),
          co_author7_id: sortedCoauthorEntries[6]?.[1]?.author?.authid,
          co_author8: extractName(sortedCoauthorEntries[7]?.[1]?.author),
          co_author8_id: sortedCoauthorEntries[7]?.[1]?.author?.authid,
          co_author9: extractName(sortedCoauthorEntries[8]?.[1]?.author),
          co_author9_id: sortedCoauthorEntries[8]?.[1]?.author?.authid,
          co_author10: extractName(sortedCoauthorEntries[9]?.[1]?.author),
          co_author10_id: sortedCoauthorEntries[9]?.[1]?.author?.authid,

          'affiliation-department': aff.department,
          'affiliation-faculty': aff.faculty,
          'affiliation-university': aff.university,
          'affiliation-city': aff.city,
          'affiliation-country': aff.country,
        };
      }),
    );

  const resultPromise = (async () => {
    const fp = await Deno.open(resultFile, {
      create: true,
      write: true,
      truncate: true,
    });
    fp.writeSync(new Uint8Array([0xef, 0xbb, 0xbf]));

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
