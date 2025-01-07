/**
 * For importing the new data.
 * Get all new data from Elsevier.
 */

import { stringify } from 'jsr:@std/csv';
import { TextLineStream } from 'jsr:@std/streams';
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
import { filter, flatMap, map, tupleZipReadableStreams } from './streams.ts';
import { readerToAsyncIterable } from './utils.ts';
import { readAsjcMap } from './asjc.ts';

const getAuthorFileName = (fileId: string) => {
  return `au-id-${fileId}.json` as const;
};

const getMetricsFileName = (fileId: string) => {
  return `metrics-au-id-${fileId}.json` as const;
};

const getScopusSearchFileName = (fileId: string) => {
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

const configName = 'pure-scopus-id-20241229';
const sortedBy = 'coverDate,-title';

const inputFile = `./target/${configName}.txt` as const;
const outputDir = `./target/output/${configName}` as const;
const catchDir = outputDir;
const resultFile = `${outputDir}/${configName}-talent-full-result.csv`;

const getAuthorCache = (fileId: string) => {
  return `${catchDir}/${getAuthorFileName(`${fileId}`)}` as const;
};

const getAuthorOuput = (fileId: string) => {
  return `${outputDir}/${getAuthorFileName(`${fileId}`)}` as const;
};

const getMetricsCache = (fileId: string) => {
  return `${catchDir}/${getMetricsFileName(`${fileId}`)}` as const;
};

const getMetricsOuput = (fileId: string) => {
  return `${outputDir}/${getMetricsFileName(`${fileId}`)}` as const;
};

const getScopusSearchCache = (fileId: string) => {
  return `${catchDir}/${getScopusSearchFileName(`${fileId}`)}` as const;
};

const getScopusSearchOuput = (fileId: string) => {
  return `${outputDir}/${getScopusSearchFileName(`${fileId}`)}` as const;
};

await Deno.mkdir(outputDir, { recursive: true });

const asjcMap = await readAsjcMap();

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
      let count = 1;

      return map((value) => {
        const [id, ...rest] = value.split('\t').map((term) => term.trim());

        const index = `${count++}`.padStart(5, ' ');
        console.info(`start [${index}]:`, id);

        return { id, index, rest };
      });
    })(),
  );

const [inputAuthorStream, inputMetricsAndScopusSearchStream] =
  inputStream.tee();
const [inputMetricsStream, inputScopusSearchStream] =
  inputMetricsAndScopusSearchStream.tee();

const [authorStream, auhtorCachingStream] = inputAuthorStream
  .pipeThrough(
    map(async ({ id, index, rest }) => {
      const query = `AU-ID(${id})`;
      console.info(`\t[${index}] loading author: ${query}`);

      try {
        if (id === '') {
          throw new Error(`The scopus-id for index: "${index}" is empty`);
        }

        let isCached = true;

        const body = await (async () => {
          try {
            return JSON.parse(
              await Deno.readTextFile(getAuthorCache(id)),
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
        return {
          index,
          id,
          rest,
          isCached,
          body,
          type: 'result' as const,
        };
      } catch (err) {
        console.error(`\t[${index}] error`, err);
        return {
          index,
          id,
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

const auhtorCachingPromise = auhtorCachingStream.pipeTo(
  new WritableStream({
    async write({ index, id, isCached, body }) {
      if (isCached && getAuthorCache(id) === getAuthorOuput(id)) {
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
      const fileId = id === '' ? `${indexPrefix}-${index}` : id;
      const prefix = isError ? `error-${indexPrefix}-` : '';
      const fpOut = await Deno.open(getAuthorOuput(`${prefix}${fileId}`), {
        create: true,
        write: true,
      });
      await fpOut.write(dataArray);
      fpOut.close();
      console.info(`\t[${index}] done`);
    },
  }),
);

const [metricsStream, metricsCachingStream] = inputMetricsStream
  .pipeThrough(
    map(async ({ id, index, rest }) => {
      const query = `AU-ID(${id})`;
      console.info(`\t[${index}] loading metrics: ${query}`);

      try {
        if (id === '') {
          throw new Error(`The scopus-id for index: "${index}" is empty`);
        }

        let isCached = true;

        const body = await (async () => {
          try {
            return JSON.parse(
              await Deno.readTextFile(getMetricsCache(id)),
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
                  console.info(
                    `\t[${index}] rateLimit: ${remaining?.padStart(
                      5,
                      ' ',
                    )}/${limit} reset: ${reset} [${status}]`,
                  ),
              },
            );
          }
        })();

        console.info(`\t[${index}] loaded`);
        return {
          index,
          id,
          rest,
          isCached,
          body,
          type: 'result' as const,
        };
      } catch (err) {
        console.error(`\t[${index}] error`, err);
        return {
          index,
          id,
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
  new WritableStream({
    async write({ index, id, isCached, body }) {
      if (isCached && getMetricsCache(id) === getMetricsOuput(id)) {
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
      const fileId = id === '' ? `${indexPrefix}-${index}` : id;
      const prefix = isError ? `error-${indexPrefix}-` : '';
      const fpOut = await Deno.open(getMetricsOuput(`${prefix}${fileId}`), {
        create: true,
        write: true,
      });
      await fpOut.write(dataArray);
      fpOut.close();
      console.info(`\t[${index}] done`);
    },
  }),
);

const [scopusSearchStream, scopusSearchCachingStream] = inputScopusSearchStream
  .pipeThrough(
    map(async ({ id, index, rest }) => {
      console.info(`start [${index}]:`, id);
      // const query = `AU-ID(${id}) AND FIRSTAUTH(${name})`;
      const query = `AU-ID(${id})`;
      console.info(`\t[${index}] loading: ${query}`);
      try {
        if (id === '') {
          throw new Error(`The scopus-id for index: "${index}" is empty`);
        }

        let isCached = true;

        const body = await (async () => {
          try {
            return JSON.parse(
              await Deno.readTextFile(getScopusSearchCache(id)),
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
          rest,
          isCached,
          body,
          type: 'result' as const,
        };
      } catch (err: unknown) {
        console.error(`\t[${index}] error`, err);
        return {
          index,
          id,
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
  new WritableStream({
    async write({ index, id, isCached, body }) {
      if (isCached && getScopusSearchCache(id) === getScopusSearchOuput(id)) {
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
      const fileId = id === '' ? `${indexPrefix}-${index}` : id;
      const prefix = isError ? `error-${indexPrefix}-` : '';
      const fpOut = await Deno.open(
        getScopusSearchOuput(`${prefix}${fileId}`),
        {
          create: true,
          write: true,
        },
      );
      await fpOut.write(dataArray);
      fpOut.close();
      console.info(`\t[${index}] done`);
    },
  }),
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
      console.log(
        `Combinding: ${authorBody.id}:${metricsBody.id}:${scopusSearchBody.id}`,
      );
      if (
        authorBody.id !== metricsBody.id ||
        authorBody.id !== scopusSearchBody.id
      ) {
        throw 'ID miss match';
      }

      // const [givenName, surname, _, nPub, searchingSubjectArea] =
      //   authorBody.rest;

      const authorResult = authorBody.body['author-retrieval-response'][0];
      const metrics = metricsBody.body.results[0].metrics;
      const scopusSearchResults = scopusSearchBody.body['search-results'].entry;

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

      const asjcData = asjcMap.get(asjcCode) ?? {
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

      const sortedKeywordEntries = [...keywordMap.entries()].sort((pre, next) =>
        pre[1] > next[1] ? -1 : pre[1] < next[1] ? 1 : 0,
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
      const surname = authorResult['author-profile']['preferred-name'].surname;

      const ipDoc = ((currentAffiliation) =>
        (Array.isArray(currentAffiliation)
          ? currentAffiliation.find(
              (affiliation) => affiliation['ip-doc']['@type'] === 'dept',
            ) ?? currentAffiliation[0]
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
        id: authorBody.id,
        'given-name': givenName,
        surname,
        name: authorBody.body['author-retrieval-response'][0]['author-profile'][
          'preferred-name'
        ]['indexed-name'],
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
