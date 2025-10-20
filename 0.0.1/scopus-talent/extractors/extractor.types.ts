import { AuthorMetricsResponseBody } from '../../elsevier-types/sci-val-author-types.ts';
import { ScopusAuthorResponseBody } from '../../elsevier-types/scopus-author-types.ts';
import { ScopusSearchEntry } from '../../elsevier-types/scopus-search-types.ts';
import { ScopusSearchResponseBody } from '../../elsevier-types/scopus-types.ts';
import { GenerateFetchedStreamData } from '../generator.types.ts';

export type ExtractFn<
  R extends { [key: string]: unknown } = { [key: string]: unknown },
> = ([authorBody, metricsBody, scopusSearchBody]: [
  GenerateFetchedStreamData<'result', ScopusAuthorResponseBody>,
  GenerateFetchedStreamData<'result', AuthorMetricsResponseBody>,
  GenerateFetchedStreamData<
    'result',
    ScopusSearchResponseBody<ScopusSearchEntry>
  >,
]) => R;
