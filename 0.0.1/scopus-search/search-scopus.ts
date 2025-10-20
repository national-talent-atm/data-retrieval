import { ScopusSearchApi } from '../elsevier-apis/scopus-search-api.ts';
import { ScopusClient } from '../elsevier-clients/scopus-client.ts';

export async function search(
  targetDir: string,
  apiKeys: readonly string[],
  query: string,
): Promise<void> {
  const client = new ScopusClient(apiKeys, 10);
  const scopusSearchApi = new ScopusSearchApi(client);

  scopusSearchApi.search({
    query,
  });
}
