import {
  ScopusAddress,
  ScopusClassification,
  ScopusDate,
  ScopusId,
  ScopusLink,
  ScopusName,
  ScopusRange,
  ScopusSubjectArea,
  ScopusType,
} from './scopus-types.ts';

export type ScopusAuthorCoredata = {
  'prism:url': string;
  'dc:identifier': string;
  'document-count': string;
  'cited-by-count': string;
  'citation-count': string;
  link: ScopusLink[];
};

export type ScopusAffiliationHistory = {
  affiliation: ScopusId[];
};

export type ScopusSubjectAreas = {
  'subject-area': ScopusSubjectArea[];
};

export type ScopusClassificationGroup = {
  classifications: ScopusType<'@type'> & {
    classification: ScopusClassification | ScopusClassification[];
  };
};

export type ScopusJournal = ScopusType<'@type'> & {
  sourcetitle?: string;
  'sourcetitle-abbrev'?: string;
  issn?: string;
};

export type ScopusJournalHistory = ScopusType<'@type'> & {
  journal: ScopusJournal[];
};

export type ScopusIpDocName = ScopusType<'@source' | '$'>;

export type ScopusBaseIpDoc = ScopusType<'@id' | '@type' | '@relationship'> & {
  afdispname: string;
  'preferred-name': ScopusIpDocName;
  'sort-name': string;
  address: ScopusAddress;
};

export type ScopusParentIpDoc = ScopusBaseIpDoc & {
  '@type': 'parent';
};

export type ScopusDeptIpDoc = ScopusBaseIpDoc & {
  '@type': 'dept';
  'parent-preferred-name': ScopusIpDocName;
};

export type ScopusIpDoc = ScopusParentIpDoc | ScopusDeptIpDoc;

export type ScopusProfileAffiliation = ScopusType<
  '@affiliation-id' | '@parent'
> & {
  'ip-doc': ScopusIpDoc;
};

export type ScopusProfileAffiliationHistory = {
  affiliation: ScopusProfileAffiliation[];
};

export type ScopusAuthorProfile = {
  status: string;
  'date-created': ScopusDate;
  'preferred-name': ScopusName;
  'name-variant': ScopusName[];
  classificationgroup: ScopusClassificationGroup;
  'publication-range': ScopusRange;
  'journal-history': ScopusJournalHistory;
  'affiliation-current'?: {
    affiliation: ScopusProfileAffiliation | ScopusProfileAffiliation[];
  };
  'affiliation-history': ScopusProfileAffiliationHistory;
};

export type ScopusAuthorResponse = {
  coredata: ScopusAuthorCoredata;
  'affiliation-current': ScopusId;
  'affiliation-history': ScopusAffiliationHistory;
  'subject-areas': ScopusSubjectAreas;
  'author-profile': ScopusAuthorProfile;
  'h-index': string;
  'coauthor-count': string;
};

export type ScopusAuthorResponseBody = {
  'author-retrieval-response': ScopusAuthorResponse[];
};
