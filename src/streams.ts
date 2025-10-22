import { toTransformStream } from 'jsr:@std/streams@1.0.13';
import { ReadableStreamTuple } from './utility-types.ts';

export const zeroCopyQueuingStrategy = new CountQueuingStrategy({
  highWaterMark: 0,
});

/**
 * Filter out unwanted value.
 *
 * @param predicate
 * @param writableStrategy
 * @param readableStrategy
 * @returns
 */
export function filter<I, O extends I>(
  predicate: (value: I) => value is O,
  writableStrategy?: QueuingStrategy<I>,
  readableStrategy?: QueuingStrategy<O>,
): TransformStream<I, O>;

export function filter<I>(
  predicate: (value: I) => unknown,
  writableStrategy?: QueuingStrategy<I>,
  readableStrategy?: QueuingStrategy<I>,
): TransformStream<I, I>;

export function filter<I, O extends I>(
  predicate: (value: I) => unknown,
  writableStrategy?: QueuingStrategy<I>,
  readableStrategy?: QueuingStrategy<O>,
): TransformStream<I, O> {
  return toTransformStream(
    async function* (stream) {
      for await (const value of stream) {
        if (predicate(value)) {
          yield value as O;
        }
      }
    },
    writableStrategy,
    readableStrategy,
  );
}

/**
 * Map main stream to new value.
 *
 * @param callbackFn
 * @param writableStrategy
 * @param readableStrategy
 * @returns
 */

export function map<I, O>(
  callbackFn: (value: I, index: number) => O | Promise<O>,
  writableStrategy?: QueuingStrategy<I>,
  readableStrategy?: QueuingStrategy<O>,
): TransformStream<I, O> {
  return toTransformStream<I, O>(
    async function* (stream) {
      let index = 0;
      for await (const value of stream) {
        yield await callbackFn(value, index++);
      }
    },
    writableStrategy,
    readableStrategy,
  );
}

/**
 * Blocking merge sub-stream to main stream.
 * If there is any error occured, stream will be breaked.
 * No data race occur.
 *
 * @param callbackFn
 * @param writableStrategy
 * @param readableStrategy
 * @returns
 */
export function flatMap<I, O>(
  callbackFn: (value: I) => ReadableStream<O> | Promise<ReadableStream<O>>,
  writableStrategy?: QueuingStrategy<I>,
  readableStrategy?: QueuingStrategy<O>,
): TransformStream<I, O> {
  return toTransformStream(
    async function* (stream) {
      for await (const value of stream) {
        yield* await callbackFn(value);
      }
    },
    writableStrategy,
    readableStrategy,
  );
}

/**
 * Non-blocking merge between each sub-stream to main stream.
 * Each sub-stream still be blocking type.
 * If each sub-stream has error others sub-streams still continue.
 * If main stream has error existing sub-streams still continue.
 * Data race may occur.
 *
 * @param callbackFn
 * @param writableStrategy
 * @param readableStrategy
 * @returns
 */
export function mergeMap<I, O>(
  callbackFn: (value: I) => ReadableStream<O> | Promise<ReadableStream<O>>,
  writableStrategy?: QueuingStrategy<I>,
  readableStrategy?: QueuingStrategy<O>,
): TransformStream<I, O> {
  let readableController: ReadableStreamDefaultController<O>;
  const subStreamSet = new Set<ReadableStream<O>>();
  let closed = false;
  let error: unknown = null;

  const closeController = () => {
    if (error) {
      readableController.error(error);
    } else {
      readableController.close();
    }
  };

  const closeStream = () => {
    closed = true;
    // NOTE: If there is no running subStream, close controller by self.
    if (subStreamSet.size === 0) {
      closeController();
    }
  };

  return {
    writable: new WritableStream<I>(
      {
        write(chunk) {
          (async () => {
            let subStream: ReadableStream<O> | null = null;
            try {
              subStream = await callbackFn(chunk);
              subStreamSet.add(subStream);

              for await (const value of subStream) {
                readableController.enqueue(value);
              }
            } catch (err: unknown) {
              console.error(err);
            } finally {
              if (subStream) {
                subStreamSet.delete(subStream);
              }

              if (closed && subStreamSet.size === 0) {
                closeController();
              }
            }
          })();
        },
        close() {
          closeStream();
        },
        abort(reason) {
          error = reason;
          closeStream();
        },
      },
      writableStrategy,
    ),
    readable: new ReadableStream<O>(
      {
        start(controller) {
          readableController = controller;
        },
      },
      readableStrategy,
    ),
  };
}

/**
 * Switch to the new sub-stream.
 * If it gets the new chunk from the main stream,
 * the new sub-stream will be created and the old sub-stream will be canceled.
 *
 * @param callbackFn
 * @param writableStrategy
 * @param readableStrategy
 * @returns
 */
export function switchMap<I, O>(
  callbackFn: (value: I) => ReadableStream<O> | Promise<ReadableStream<O>>,
  writableStrategy?: QueuingStrategy<I>,
  readableStrategy?: QueuingStrategy<O>,
): TransformStream<I, O> {
  let readableController: ReadableStreamDefaultController<O>;
  let currentSubStream: ReadableStream<O> | null = null;
  let closed = false;
  let error: unknown = null;

  const closeController = () => {
    if (error) {
      readableController.error(error);
    } else {
      readableController.close();
    }
  };

  const closeStream = () => {
    closed = true;
    // NOTE: If there is no running currentSubStream, close controller by self.
    if (currentSubStream === null) {
      closeController();
    }
  };

  return {
    writable: new WritableStream<I>(
      {
        write(chunk) {
          (async () => {
            let subStream: ReadableStream<O> | null = null;

            try {
              subStream = await callbackFn(chunk);
              currentSubStream = subStream;

              const reader = subStream.getReader();
              while (true) {
                const { value, done } = await reader.read();
                if (done) {
                  break;
                }

                if (subStream !== currentSubStream) {
                  reader.releaseLock();
                  subStream.cancel();
                  break;
                }

                readableController.enqueue(value);
              }
            } catch (err: unknown) {
              console.error(err);
            } finally {
              if (currentSubStream === subStream) {
                if (closed) {
                  closeController();
                }

                currentSubStream = null;
              }
            }
          })();
        },
        close() {
          closeStream();
        },
        abort(reason) {
          error = reason;
          closeStream();
        },
      },
      writableStrategy,
    ),
    readable: new ReadableStream<O>(
      {
        start(controller) {
          readableController = controller;
        },
      },
      readableStrategy,
    ),
  };
}

export function tupleZipReadableStreams<T extends [unknown, ...unknown[]]>(
  ...streams: ReadableStreamTuple<T>
): ReadableStream<T> {
  const readers = streams.map((s) => s.getReader());
  return new ReadableStream<T>({
    async start(controller) {
      try {
        loop: while (true) {
          const results: unknown[] = [];
          for (const reader of readers) {
            const { value, done } = await reader.read();
            if (!done) {
              results.push(value);
            } else {
              await Promise.all(readers.map((reader) => reader.cancel()));
              break loop;
            }
          }
          controller.enqueue(results as T);
        }
        controller.close();
      } catch (e) {
        controller.error(e);
      }
    },
  });
}

export function flatToStream<T>(
  promise: Promise<ReadableStream<T>>,
): Awaited<typeof promise> {
  return new ReadableStream({
    async start(controller) {
      const stream = await promise;
      await stream.pipeTo(
        new WritableStream({
          write(value) {
            controller.enqueue(value);
          },
        }),
      );
      controller.close();
    },
  });
}

export async function toArray<T>(
  readableStream: ReadableStream<T>,
): Promise<T[]> {
  const results: T[] = [];

  await readableStream.pipeTo(
    new WritableStream<T>({
      write(entry) {
        results.push(entry);
      },
    }),
  );

  return results;
}

export function multipleTee<T>(
  readableStream: ReadableStream<T>,
  length: number,
): readonly ReadableStream<T>[] {
  return Array.from({ length: length - 1 }).reduce(
    (result: ReadableStream<T>[]) => {
      const next = result.pop() as ReadableStream<T>;
      return [...result, ...next.tee()];
    },
    [readableStream],
  );
}
