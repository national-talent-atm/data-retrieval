import { TextLineStream } from 'https://deno.land/std@0.210.0/streams/mod.ts';
import { stringify } from 'https://deno.land/std@0.210.0/csv/mod.ts';
import { ScopusClient } from './elsevier-clients/scopus-client.ts';
import { filter, flatMap, map, tupleZipReadableStreams } from './streams.ts';
import { ScopusAuthorRetrievalApi } from './elsevier-apis/scopus-author-retrieval-api.ts';
import { ScopusAuthorResponseBody } from './elsevier-types/scopus-author-types.ts';
import { readerToAsyncIterable } from './utils.ts';
import { SciValAuthorApi } from './sci-val-apis/sci-val-author.ts';
import {
  AuthorMetricsResponseBody,
  MetricResult,
} from './elsevier-types/sci-val-author-types.ts';

const getAuthorFileName = (fileId: string) => {
  return `au-id-${fileId}.json` as const;
};

const getMetricsFileName = (fileId: string) => {
  return `metrics-au-id-${fileId}.json` as const;
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

const inputFile = `./target/${configName}.txt` as const;
const outputDir = `./target/output/${configName}` as const;
const catchDir = outputDir;
const resultFile = `${outputDir}/${configName}-talent-generator-result.csv`;

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

await Deno.mkdir(outputDir, { recursive: true });

const client = new ScopusClient(apiKeys, 10);
const authorRetrievalApi = new ScopusAuthorRetrievalApi(client);
const authorMetricsApi = new SciValAuthorApi(client);

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
        const [id, name, ind] = value.split('\t').map((term) => term.trim());

        const index = `${count++}`.padStart(5, ' ');
        console.info(`start [${index}]:`, id, name, ind);

        return { id, name, ind, index };
      });
    })(),
  );

const [inputAuthorStream, inputMetricsStream] = inputStream.tee();

const [authorStream, auhtorCachingStream] = inputAuthorStream
  .pipeThrough(
    map(async ({ id, name, ind, index }) => {
      const query = `AU-ID(${id})`;
      console.info(`\t[${index}] loading author: ${query}`);

      try {
        if (id === '') {
          throw new Error(`The scopus-id for "${name}" is empty`);
        }

        let isCached = true;

        const authorResult = await (async () => {
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
          name,
          ind,
          isCached,
          authorResult,
          type: 'result' as const,
        };
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
          type: 'error' as const,
        };
      }
    }),
  )
  .tee();

auhtorCachingStream.pipeTo(
  new WritableStream({
    async write({ index, id, name, isCached, authorResult }) {
      if (isCached && getAuthorCache(id) === getAuthorOuput(id)) {
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
    map(async ({ id, name, ind, index }) => {
      const query = `AU-ID(${id})`;
      console.info(`\t[${index}] loading metrics: ${query}`);

      try {
        if (id === '') {
          throw new Error(`The scopus-id for "${name}" is empty`);
        }

        let isCached = true;

        const authorResult = await (async () => {
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
          name,
          ind,
          isCached,
          authorResult,
          type: 'result' as const,
        };
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
          type: 'error' as const,
        };
      }
    }),
  )
  .tee();

metricsCachingStream.pipeTo(
  new WritableStream({
    async write({ index, id, name, isCached, authorResult }) {
      if (isCached && getMetricsCache(id) === getMetricsOuput(id)) {
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

const combinedStream = tupleZipReadableStreams(authorStream, metricsStream)
  .pipeThrough(
    filter(
      (
        tuple,
      ): tuple is [
        Exclude<(typeof tuple)[0], { type: 'error' }>,
        Exclude<(typeof tuple)[1], { type: 'error' }>,
      ] => {
        return (
          tuple[0].type !== 'error' &&
          tuple[1].type !== 'error' &&
          tuple[0].authorResult['author-retrieval-response'].length > 0 &&
          tuple[1].authorResult.results.length > 0
        );
      },
    ),
  )
  .pipeThrough(
    map(([authorBody, metricsBody]) => {
      console.log(`Combinding: ${authorBody.id}:${metricsBody.id}`);
      if (authorBody.id !== metricsBody.id) {
        throw 'ID miss match';
      }
      const authorResult =
        authorBody.authorResult['author-retrieval-response'][0];
      const metrics = metricsBody.authorResult.results[0].metrics;

      return {
        industry: authorBody.ind,
        name: authorBody.name,
        scopus_id: authorBody.id,
        h_index: authorResult['h-index'],
        fwci: extractMatric(metrics, 'FieldWeightedCitationImpact')?.value,
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
      };
    }),
  );

(async () => {
  const fp = await Deno.open(resultFile, {
    create: true,
    write: true,
    truncate: true,
  });

  let columns: string[] | null = null;

  await combinedStream
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
