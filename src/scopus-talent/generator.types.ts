export type StreamData = {
  index: number;
  id: string;
  indexText: string;
  rest: string[];
};

export type GenerateFetchedStreamData<
  T extends string,
  BT = unknown,
> = T extends unknown
  ? StreamData & {
      isCached: boolean;
      body: BT;
      type: T;
    }
  : never;

export type FetchedStreamData<BT = unknown> = GenerateFetchedStreamData<
  'result' | 'error',
  BT
>;
