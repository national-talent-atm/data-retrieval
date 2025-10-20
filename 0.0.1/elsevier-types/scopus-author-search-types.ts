import { ScopusSubjectAreas } from './scopus-author-types.ts';
import { ScopusLink, ScopusType } from './scopus-types.ts';

export type ScopusAuthorSearchEntry = ScopusType<'@_fa'> & {
  link: ScopusLink[];
  'prism:url': string;
  'dc:identifier': string;
  eid: string;
  orcid?: string;
  'preferred-name': ScopusType<'surname' | 'given-name' | 'initials'>;
  'name-variant': ScopusType<'@_fa' | 'surname' | 'given-name' | 'initials'>[];
  'document-count': string;
  'subject-areas': ScopusSubjectAreas;
  'affiliation-current': {
    'affiliation-url': string;
    'affiliation-id': string;
    'affiliation-name': string;
    'affiliation-city': string;
    'affiliation-country': string;
  };
};
