import { ScopusLink, ScopusType } from './scopus-types.ts';

export type ScopusSearchValue = ScopusType<'$'>;

export type ScopusSearchAffiliation = ScopusType<'@_fa'> & {
  'affiliation-url'?: string;
  afid?: string;
  affilname: string;
  'affiliation-city': string;
  'affiliation-country': string;
};

export type ScopusSearchAuthor = ScopusType<'@_fa' | '@seq'> & {
  'author-url': string;
  authid: string;
  authname: string;
  surname: string;
  'given-name': string;
  initials: string;
  afid: ScopusType<'@_fa' | '$'>[];
};

export type ScopusSearchEntry = ScopusType<'@_fa'> & {
  link: ScopusLink[];
  'prism:url': string;
  'dc:identifier': string;
  eid: string;
  'dc:title': string;
  'dc:creator': string;
  'prism:publicationName': string;
  'prism:eIssn': string;
  'prism:volume': string;
  'prism:pageRange': string | null;
  'prism:coverDate': string;
  'prism:coverDisplayDate': string;
  'prism:doi': string;
  pii: string;
  'dc:description'?: string;
  'citedby-count': string;
  affiliation: ScopusSearchAffiliation[];
  'prism:aggregationType': string;
  subtype: string;
  subtypeDescription: string;
  'author-count'?: ScopusType<'@limit' | '@total' | '$'>;
  author?: ScopusSearchAuthor[];
  authkeywords?: string;
  'article-number': string;
  'source-id': string;
  'fund-acr'?: string;
  'fund-no'?: string;
  'fund-sponsor'?: string;
  openaccess: string;
  openaccessFlag: boolean;
  freetoread: { value: ScopusSearchValue[] };
  freetoreadLabel: { value: ScopusSearchValue[] };
};
