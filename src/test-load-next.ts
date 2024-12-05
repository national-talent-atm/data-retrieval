import { ScopusSearchApi } from "./elsevier-apis/scopus-search-api.ts";
import { ScopusClient } from "./elsevier-clients/scopus-client.ts";

const apiKey = Deno.env.get('ELSEVIER_KEY');

if (!apiKey) {
  console.error(
    `API key must be specified by environment variable 'ELSEVIER_KEY'`,
  );
  Deno.exit(-1);
}

const apiKeys = apiKey.split(/\s*,\s*/gi).filter((value) => value !== '');
const client = new ScopusClient(apiKeys, 10);
const scopusSearchApi = new ScopusSearchApi(client);

const result = await scopusSearchApi.search({
  query: 'AU-ID(6602406924)',
  cursor: '*',
});

console.info(result);
