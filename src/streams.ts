import { toTransformStream } from 'https://deno.land/std@0.171.0/streams/mod.ts';

const zeroCopyQueue = new CountQueuingStrategy({ highWaterMark: 0 });

export function filter<T>(callbackFn: (value: T) => boolean) {
  return toTransformStream<T, T>(
    async function* (stream) {
      for await (const value of stream) {
        if (callbackFn(value)) {
          yield value;
        }
      }
    },
    undefined,
    zeroCopyQueue,
  );
}

export function map<T, R>(callbackFn: (value: T) => R) {
  return toTransformStream<T, R>(
    async function* (stream) {
      for await (const value of stream) {
        yield callbackFn(value);
      }
    },
    undefined,
    zeroCopyQueue,
  );
}

/**
 * Blocking merge sub-stream to main stream.
 * If there is any error occured, stream will be breaked.
 * No data race occur.
 *
 * @param callbackFn
 * @returns
 */
export function flatMap<T, R>(callbackFn: (value: T) => ReadableStream<R>) {
  return toTransformStream<T, R>(
    async function* (stream) {
      for await (const value of stream) {
        yield* callbackFn(value);
      }
    },
    undefined,
    zeroCopyQueue,
  );
}

/**
 * Non-blocking merge between each sub-stream to main stream.
 * Each sub-stream still be the same blocking type as origin.
 * If each sub-stream has error others sub-streams still continue.
 * If main stream has error existing sub-streams still continue.
 * Data race may occur.
 *
 * @param callbackFn
 * @returns
 */
export function mergeMap<T, R>(callbackFn: (value: T) => ReadableStream<R>) {
  let readableController: ReadableStreamDefaultController<R>;
  const subStreamSet = new Set<ReadableStream<R>>();
  let closed = false;
  let error: unknown = null;

  return {
    writable: new WritableStream<T>({
      write(chunk) {
        (async () => {
          let subStream: ReadableStream<R> | null = null;
          try {
            subStream = callbackFn(chunk);
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
              if (error) {
                readableController.error(error);
              } else {
                readableController.close();
              }
            }
          }
        })();
      },
      close() {
        closed = true;
      },
      abort(reason) {
        closed = true;
        error = reason;
      },
    }),
    readable: new ReadableStream<R>(
      {
        start(controller) {
          readableController = controller;
        },
      },
      zeroCopyQueue,
    ),
  };
}

/**
 * Switch to the new sub-stream.
 * If it gets the new chunk from the main stream,
 * the new sub-stream will be created and the old sub-stream will be canceled.
 *
 * @param callbackFn
 * @returns
 */
export function switchMap<T, R>(callbackFn: (value: T) => ReadableStream<R>) {
  let readableController: ReadableStreamDefaultController<R>;
  let currentSubStream: ReadableStream<R> | null = null;
  let closed = false;
  let error: unknown = null;

  return {
    writable: new WritableStream<T>({
      write(chunk) {
        (async () => {
          let subStream: ReadableStream<R> | null = null;

          try {
            subStream = callbackFn(chunk);
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
            if (closed && currentSubStream === subStream) {
              if (error) {
                readableController.error(error);
              } else {
                readableController.close();
              }
            }
          }
        })();
      },
      close() {
        closed = true;
      },
      abort(reason) {
        closed = true;
        error = reason;
      },
    }),
    readable: new ReadableStream<R>(
      {
        start(controller) {
          readableController = controller;
        },
      },
      zeroCopyQueue,
    ),
  };
}
