import {
  RateLimitNotify,
  ScopusClient,
} from '../elsevier-clients/scopus-client.ts';
import {
  AuthorMetricsResponseBody,
  MetricType,
} from '../elsevier-types/sci-val-author-types.ts';
import { NonEmptyArray } from '../utility-types.ts';

export type YearRange =
  | '3yrs'
  | '3yrsAndCurrent'
  | '3yrsAndCurrentAndFuture'
  | '5yrs'
  | '5yrsAndCurrent'
  | '5yrsAndCurrentAndFuture'
  | '10yrs';

export type IncludedDoc =
  | 'AllPublicationTypes'
  | 'ArticlesOnly'
  | 'ArticlesReviews'
  | 'ArticlesReviewsConferencePapers'
  | 'ArticlesReviewsConferencePapersBooksAndBookChapters'
  | 'ConferencePapersOnly'
  | 'ArticlesConferencePapers'
  | 'BooksAndBookChapters';

export type JournalImpactType = 'CiteScore' | 'SNIP' | 'SJR';

export type IndexType = 'hIndex' | 'h5Index' | 'gIndex' | 'mIndex';

export type AuthorMatricsOptions = {
  queries?: {
    yearRange?: YearRange;
    subjectAreaFilterURI?: string;
    includeSelfCitations?: boolean;
    byYear?: boolean;
    includedDocs?: NonEmptyArray<IncludedDoc>;
    journalImpactType?: JournalImpactType;
    showAsFieldWeighted?: boolean;
    indexType?: IndexType;
  };

  rateLimitNotify?: RateLimitNotify;
};

export const sciValAuthorUrl =
  'https://api.elsevier.com/analytics/scival/author/' as const;

export class SciValAuthorApi {
  constructor(private readonly client: ScopusClient) {}

  async metrics(
    authorIds: NonEmptyArray<string>,
    matricTypes: NonEmptyArray<MetricType>,
    { queries, rateLimitNotify }: AuthorMatricsOptions,
  ): Promise<AuthorMetricsResponseBody> {
    const url = new URL(`metrics`, sciValAuthorUrl);

    url.searchParams.set('authors', authorIds.join(','));
    url.searchParams.set('metricTypes', matricTypes.join(','));

    if (queries) {
      for (const [key, value] of Object.entries(queries)) {
        url.searchParams.set(
          key,
          Array.isArray(value) ? value.join(',') : `${value}`,
        );
      }
    }

    return await this.client.get<AuthorMetricsResponseBody>(
      url,
      rateLimitNotify,
    );
  }
}
