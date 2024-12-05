import {
  RateLimitNotify,
  ScopusClient,
} from '../elsevier-clients/scopus-client.ts';
import { ScopusSearchEntry } from '../elsevier-types/scopus-search-types.ts';
import {
  ScopusSearchResponseBody,
  ScopusSubjectAreaAbbrev,
} from '../elsevier-types/scopus-types.ts';

const scopusSearchUrl = 'https://api.elsevier.com/content/search/scopus';

export type ScopusSearchView = 'STANDARD' | 'COMPLETE';

export type ScopusSearchOptions = {
  query: string;
  view?: ScopusSearchView;
  field?: string;
  suppressNavLinks?: boolean;
  date?: string;
  start?: number;
  count?: number;
  sort?: string;
  content?: 'core' | 'dummy' | 'all';
  subj?: ScopusSubjectAreaAbbrev;
  alias?: boolean;
  cursor?: string;
  facets?: string;
};

export class ScopusSearchApi {
  constructor(private readonly client: ScopusClient) {}

  async search(
    options: ScopusSearchOptions,
    rateLimitNotify?: RateLimitNotify,
  ): Promise<ScopusSearchResponseBody<ScopusSearchEntry>> {
    let url = new URL('', scopusSearchUrl);

    for (const [key, value] of Object.entries(options)) {
      url.searchParams.set(key, `${value}`);
    }

    let previousResult!: ScopusSearchResponseBody<ScopusSearchEntry>;

    for(let i = 0; i < 100; i++) {
      const result = await this.client.get<ScopusSearchResponseBody<ScopusSearchEntry>>(
        url,
        rateLimitNotify,
      );

      if(typeof previousResult === 'undefined') {
        previousResult = result;
      } else {
        result['search-results']['entry'] = [...previousResult['search-results']['entry'], ...result['search-results']['entry']];
        previousResult = result;
      }

      if(previousResult['search-results']['entry'].length >= +previousResult['search-results']['opensearch:totalResults']) {
        break;
      }

      const nextUrl = result['search-results']['link'].find((link) => link['@ref'] === 'next')?.['@href'];

      if(typeof nextUrl === 'undefined') {
        break;
      }

      url = new URL('', nextUrl);
    }

    return previousResult;
  }
}
