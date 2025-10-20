import asjc from '../../asjc.json' with { type: 'json' };
import {
  AuthorMetricsResponseBody,
  MetricResult,
} from '../../elsevier-types/sci-val-author-types.ts';
import { ScopusAuthorResponseBody } from '../../elsevier-types/scopus-author-types.ts';
import {
  ScopusSearchAuthor,
  ScopusSearchEntry,
} from '../../elsevier-types/scopus-search-types.ts';
import {
  ScopusName,
  ScopusSearchResponseBody,
} from '../../elsevier-types/scopus-types.ts';
import { GenerateFetchedStreamData } from '../generator.types.ts';

type AsjcCode = keyof typeof asjc;

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

export type AdditionalDataFn = (args: {
  index: number;
  id: string;
  rest: string[];
  scopusAuthorResponseBody: ScopusAuthorResponseBody;
  authorMetricsResponseBody: AuthorMetricsResponseBody;
  scopusSearchResponseBody: ScopusSearchResponseBody<ScopusSearchEntry>;
}) => { [key: string]: unknown };

export function generateExtractAllFn({
  additionalDataFn = undefined as AdditionalDataFn | undefined,
} = {}) {
  return ([authorBody, metricsBody, scopusSearchBody]: [
    GenerateFetchedStreamData<'result', ScopusAuthorResponseBody>,
    GenerateFetchedStreamData<'result', AuthorMetricsResponseBody>,
    GenerateFetchedStreamData<
      'result',
      ScopusSearchResponseBody<ScopusSearchEntry>
    >,
  ]) => {
    const { index, id, indexText, rest } = authorBody;

    console.log(
      `\t%c[${indexText}] 🎶 extracting: AUTHOR:${authorBody.id} | METRICS:${metricsBody.id} | SCOPUS-SEARCH:${scopusSearchBody.id}`,
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
    const scopusSearchResults = scopusSearchBody.body['search-results'].entry;

    const asjcSorted = ((classifications) =>
      classifications['@type'] === 'ASJC'
        ? Array.isArray(classifications['classification'])
          ? classifications['classification']
          : [classifications['classification']]
        : [])(
      authorResult['author-profile']['classificationgroup']['classifications'],
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
      typeof currentAffiliation === 'undefined'
        ? undefined
        : (Array.isArray(currentAffiliation)
            ? (currentAffiliation.find(
                (affiliation) => affiliation['ip-doc']['@type'] === 'dept',
              ) ?? currentAffiliation[0])
            : currentAffiliation)['ip-doc'])(
      authorResult['author-profile']['affiliation-current']?.affiliation,
    );

    const aff = ((ipDoc) => {
      if (typeof ipDoc === 'undefined') {
        return undefined;
      }

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

      'affiliation-department': aff?.department,
      'affiliation-faculty': aff?.faculty,
      'affiliation-university': aff?.university,
      'affiliation-city': aff?.city,
      'affiliation-country': aff?.country,
    };
  };
}
