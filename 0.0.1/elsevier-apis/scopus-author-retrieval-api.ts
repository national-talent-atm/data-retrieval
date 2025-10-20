import {
  RateLimitNotify,
  ScopusClient,
} from '../elsevier-clients/scopus-client.ts';
import { ScopusAuthorResponseBody } from '../elsevier-types/scopus-author-types.ts';

export const scopusAuthorIdUrl =
  'https://api.elsevier.com/content/author/author_id/';

export type ScopusAuthorIdView =
  | 'LIGHT'
  | 'STANDARD'
  | 'ENHANCED'
  | 'METRICS'
  | 'DOCUMENTS'
  | 'ENTITLED'
  | 'ORCID'
  | 'ORCID_BIO'
  | 'ORCID_WORKS';

export type ScopusAuthorIdOptions = {
  view?: ScopusAuthorIdView;
  field?: string;
  alias?: boolean;
};

export class ScopusAuthorRetrievalApi {
  constructor(private readonly client: ScopusClient) {}

  async authorId(
    authorId: string,
    options?: ScopusAuthorIdOptions,
    rateLimitNotify?: RateLimitNotify,
  ): Promise<ScopusAuthorResponseBody> {
    const url = new URL(authorId, scopusAuthorIdUrl);

    if (options) {
      for (const [key, value] of Object.entries(options)) {
        url.searchParams.set(key, `${value}`);
      }
    }

    return await this.client.get<ScopusAuthorResponseBody>(
      url,
      rateLimitNotify,
    );
  }
}
