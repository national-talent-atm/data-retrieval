export const scopusAuthorUrl =
  'https://api.elsevier.com/content/author/author_id/' as const;

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

export type RateLimitNotify = (
  limit: string | null,
  remaining: string | null,
  reset: string | null,
  status: string | null,
) => void;

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
    rateLimitNotify?: RateLimitNotify,
  ): Promise<T> {
    await this.delay();

    const headers = new Headers(init.headers);
    headers.set('Accept', 'application/json');
    headers.set('X-ELS-APIKey', this.apiKey);

    const reqestInit = {
      ...init,
      headers: headers,
    };

    const res = await fetch(input, reqestInit);

    if (rateLimitNotify) {
      const { headers } = res;

      rateLimitNotify(
        headers.get('X-RateLimit-Limit'),
        headers.get('X-RateLimit-Remaining'),
        headers.get('X-RateLimit-Reset'),
        headers.get('X-ELS-Status'),
      );
    }

    if (!res.ok) {
      throw new Error(res.statusText, {
        cause: await (async () => {
          try {
            return await res.json();
          } catch {
            return await res.text();
          }
        })(),
      });
    }

    return (await res.json()) as T;
  }

  async get<T>(url: URL, rateLimitNotify?: RateLimitNotify): Promise<T> {
    return await this.fetch<T>(url, {}, rateLimitNotify);
  }

  async post<T>(
    url: URL,
    body: BodyInit,
    rateLimitNotify?: RateLimitNotify,
  ): Promise<T> {
    return await this.fetch<T>(
      url,
      {
        method: 'POST',
        body: body,
      },
      rateLimitNotify,
    );
  }
}
