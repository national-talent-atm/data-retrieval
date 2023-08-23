import { Optional } from '../utility-types.ts';

export type ScopusType<T extends string> = {
  [K in T]: string;
};

export type ScopusId = Optional<ScopusType<'@id' | '@href' | '@_fa'>, '@_fa'>;

export type ScopusDate = ScopusType<'@day' | '@month' | '@year'>;

export type ScopusLink = Optional<
  ScopusType<'@_fa' | '@rel' | '@ref' | '@href' | '@type'>,
  '@_fa' | '@rel' | '@ref' | '@type'
>;

export type ScopusSubjectArea = Optional<
  ScopusType<'@_fa' | '@abbrev' | '@code' | '$'>,
  '@_fa'
>;

export type ScopusClassification = ScopusType<'@frequency' | '$'>;

export type ScopusRange = ScopusType<'@end' | '@start'>;

export type ScopusName = {
  initials: string;
  'indexed-name': string;
  surname: string;
  'given-name': string;
};

export type ScopusAddress = ScopusType<'@country'> & {
  '@country': string;
  'address-part': string;
  city: string;
  'postal-code': string;
  country: string;
};

export type ScopusSubjectAreaAbbrev =
  | 'AGRI' // Agricultural and Biological Sciences
  | 'ARTS' // Arts and Humanities
  | 'BIOC' // Biochemistry, Genetics and Molecular Biology
  | 'BUSI' // Business, Management and Accounting
  | 'CENG' // Chemical Engineering
  | 'CHEM' // Chemistry
  | 'COMP' // Computer Science
  | 'DECI' // Decision Sciences
  | 'DENT' // Dentistry
  | 'EART' // Earth and Planetary Sciences
  | 'ECON' // Economics, Econometrics and Finance
  | 'ENER' // Energy
  | 'ENGI' // Engineering
  | 'ENVI' // Environmental Science
  | 'HEAL' // Health Professions
  | 'IMMU' // Immunology and Microbiology
  | 'MATE' // Materials Science
  | 'MATH' // Mathematics
  | 'MEDI' // Medicine
  | 'NEUR' // Neuroscience
  | 'NURS' // Nursing
  | 'PHAR' // Pharmacology, Toxicology and Pharmaceutics
  | 'PHYS' // Physics and Astronomy
  | 'PSYC' // Psychology
  | 'SOCI' // Social Sciences
  | 'VETE' // Veterinary
  | 'MULT'; // Multidisciplinary
