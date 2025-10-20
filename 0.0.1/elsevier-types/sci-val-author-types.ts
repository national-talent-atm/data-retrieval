export type Link = {
  readonly '@ref': string;
  readonly '@href': string;
  readonly '@type': string;
};

export type MetricType =
  | 'AcademicCorporateCollaboration'
  | 'AcademicCorporateCollaborationImpact'
  | 'Collaboration'
  | 'CitationCount'
  | 'CitationsPerPublication'
  | 'CollaborationImpact'
  | 'CitedPublications'
  | 'FieldWeightedCitationImpact'
  | 'HIndices'
  | 'ScholarlyOutput'
  | 'PublicationsInTopJournalPercentiles'
  | 'OutputsInTopCitationPercentiles';

interface BaseMetricResult<MT extends MetricType> {
  readonly metricType: MT;
}

interface MetricValue {
  readonly value: number;
}

interface MetricValues<VT> {
  readonly values: readonly VT[];
}

interface MetricPercentage {
  readonly percentage: number;
}

interface Collaboration extends MetricValue {
  readonly collabType: string;
}

interface CollaborationWithPercentage extends Collaboration, MetricPercentage {}

interface Threshold extends MetricValue, MetricPercentage {
  readonly threshold: number;
}

export interface AcademicCorporateCollaborationResult
  extends BaseMetricResult<'AcademicCorporateCollaboration'>,
    MetricValues<CollaborationWithPercentage> {}

export interface AcademicCorporateCollaborationImpactResult
  extends BaseMetricResult<'AcademicCorporateCollaborationImpact'>,
    MetricValues<Collaboration> {}

export interface CollaborationResult
  extends BaseMetricResult<'Collaboration'>,
    MetricValues<CollaborationWithPercentage> {}

export interface CitationCountResult
  extends BaseMetricResult<'CitationCount'>,
    MetricValue {}

export interface CitationsPerPublicationResult
  extends BaseMetricResult<'CitationsPerPublication'>,
    MetricValue {}

export interface CollaborationImpactResult
  extends BaseMetricResult<'CollaborationImpact'>,
    MetricValues<Collaboration> {}

export interface CitedPublicationsResult
  extends BaseMetricResult<'CitedPublications'>,
    MetricValue,
    MetricPercentage {}

export interface FieldWeightedCitationImpactResult
  extends BaseMetricResult<'FieldWeightedCitationImpact'>,
    MetricValue {}

export interface HIndicesResult
  extends BaseMetricResult<'HIndices'>,
    MetricValue {
  readonly indexType: string;
}

export interface ScholarlyOutputResult
  extends BaseMetricResult<'ScholarlyOutput'>,
    MetricValue {}

export interface PublicationsInTopJournalPercentilesResult
  extends BaseMetricResult<'PublicationsInTopJournalPercentiles'>,
    MetricValues<Threshold> {
  impactType: string;
}

export interface OutputsInTopCitationPercentilesResult
  extends BaseMetricResult<'OutputsInTopCitationPercentiles'>,
    MetricValues<Threshold> {}

export type MetricResult =
  | AcademicCorporateCollaborationResult
  | AcademicCorporateCollaborationImpactResult
  | CollaborationResult
  | CitationCountResult
  | CitationsPerPublicationResult
  | CollaborationImpactResult
  | CitedPublicationsResult
  | FieldWeightedCitationImpactResult
  | HIndicesResult
  | ScholarlyOutputResult
  | PublicationsInTopJournalPercentilesResult
  | OutputsInTopCitationPercentilesResult;

export type AuthorMetricsResponseBody = {
  readonly link: Link;

  readonly dataSource: {
    readonly sourceName: string;
    readonly lastUpdated: string;
    readonly metricStartYear: number;
    readonly metricEndYear: number;
  };

  readonly results: {
    readonly metrics: readonly MetricResult[];
    readonly author: {
      readonly link: Link;
      readonly id: number;
      readonly name: string;
      readonly uri: string;
    };
  }[];
};
