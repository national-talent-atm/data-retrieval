import {
  RateLimitNotify,
  ScopusClient,
} from '../elsevier-clients/scopus-client.ts';
import { ScopusAuthorSearchEntry } from '../elsevier-types/scopus-author-search-types.ts';
import { ScopusSearchResponseBody } from '../elsevier-types/scopus-types.ts';

const scopusAuthorSearchUrl = 'https://api.elsevier.com/content/search/author';

export type ScopusAuthorSearchView = 'STANDARD';

interface ScopusAuthorSearchBaseOptions {
  view?: ScopusAuthorSearchView;
  field?: string;
  suppressNavLinks?: boolean;
  date?: string;
  start?: number;
  count?: number;
  sort?: string;
  facets?: string;
  alias?: boolean;
}

export interface ScopusAuthorSearchQueryOptions
  extends ScopusAuthorSearchBaseOptions {
  query: string;
}

export interface ScopusAuthorSearchCoauthorOptions
  extends ScopusAuthorSearchBaseOptions {
  'co-author': string;
}

export type ScopusAuthorSearchOptions =
  | ScopusAuthorSearchQueryOptions
  | ScopusAuthorSearchCoauthorOptions;

export class ScopusAuthorSearchApi {
  constructor(private readonly client: ScopusClient) {}

  async search(
    options: ScopusAuthorSearchOptions,
    rateLimitNotify?: RateLimitNotify,
  ): Promise<ScopusSearchResponseBody<ScopusAuthorSearchEntry>> {
    const url = new URL('', scopusAuthorSearchUrl);

    for (const [key, value] of Object.entries(options)) {
      url.searchParams.set(key, `${value}`);
    }

    return await this.client.get<
      ScopusSearchResponseBody<ScopusAuthorSearchEntry>
    >(url, rateLimitNotify);
  }
}
