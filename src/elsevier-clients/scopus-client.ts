export const scopusAuthorUrl =
  'https://api.elsevier.com/content/author/author_id/';

export type ScopusClientParamTuple = ConstructorParameters<typeof ScopusClient>;

export type ScopusAuthorView =
  | 'LIGHT'
  | 'STANDARD'
  | 'ENHANCED'
  | 'METRICS'
  | 'DOCUMENTS'
  | 'ENTITLED'
  | 'ORCID'
  | 'ORCID_BIO'
  | 'ORCID_WORKS';

export type ScopusAuthorOptions = {
  view?: ScopusAuthorView;
  field?: string;
  alias?: boolean;
};

export class ScopusClient {
  private readonly nextInterval: number;
  private nextFetchTime = Date.now();

  constructor(private readonly apiKey: string, rateLimit: number) {
    this.nextInterval = Math.ceil(1000 / rateLimit);
  }

  private delay(): Promise<void> {
    return new Promise<void>((resolve) => {
      const currentTime = Date.now();
      if (this.nextFetchTime < currentTime) {
        this.nextFetchTime = currentTime;
      }
      setTimeout(() => resolve(), this.nextFetchTime - currentTime);
      this.nextFetchTime += this.nextInterval;
    });
  }

  private async fetch<T>(
    input: URL | Request | string,
    init: RequestInit = {},
  ): Promise<T> {
    await this.delay();

    const headers = new Headers(init.headers);
    headers.set('Accept', 'application/json');
    headers.set('X-ELS-APIKey', this.apiKey);

    init.headers = headers;

    const res = await fetch(input, init);

    if (!res.ok) {
      throw new Error(res.statusText, { cause: await res.text() });
    }

    return (await res.json()) as T;
  }

  async get<T>(url: URL): Promise<T> {
    return await this.fetch<T>(url);
  }

  async post<T>(url: URL, body: BodyInit): Promise<T> {
    return await this.fetch<T>(url, {
      method: 'POST',
      body: body,
    });
  }
}
