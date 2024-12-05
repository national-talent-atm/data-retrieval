export type Optional<T, K extends keyof T> = Pick<Partial<T>, K> & Omit<T, K>;

export type NonEmptyArray<T> = [T, ...T[]];

export function isNonEmptyArray<T>(arr: T[]): arr is NonEmptyArray<T> {
  return arr.length > 0;
}

export type ReadableStreamTuple<T extends unknown[]> = {
  [K in keyof T]: ReadableStream<T[K]>;
};
