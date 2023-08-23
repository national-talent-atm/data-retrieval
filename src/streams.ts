import { toTransformStream } from 'https://deno.land/std@0.186.0/streams/mod.ts';

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
  callbackFn: (value: I) => O | Promise<O>,
  writableStrategy?: QueuingStrategy<I>,
  readableStrategy?: QueuingStrategy<O>,
): TransformStream<I, O> {
  return toTransformStream<I, O>(
    async function* (stream) {
      for await (const value of stream) {
        yield await callbackFn(value);
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
