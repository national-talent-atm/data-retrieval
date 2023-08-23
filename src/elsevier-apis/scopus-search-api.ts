import { ScopusClient } from '../elsevier-clients/scopus-client.ts';
import {
  ScopusSearchResponseBody,
  ScopusSearchResults,
} from '../elsevier-types/scopus-search-types.ts';
import { ScopusSubjectAreaAbbrev } from '../elsevier-types/scopus-types.ts';

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

  async search(options: ScopusSearchOptions): Promise<ScopusSearchResults> {
    const url = new URL('', scopusSearchUrl);

    for (const [key, value] of Object.entries(options)) {
      url.searchParams.set(key, `${value}`);
    }

    const body = await this.client.get<ScopusSearchResponseBody>(url);

    return body['search-results'];
  }
}
