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

export function mergeMap<T, R>(callbackFn: (value: T) => ReadableStream<R>) {
  let readableController: ReadableStreamDefaultController<R>;
  const subStreamSet = new Set<ReadableStream<R>>();
  let closed = false;
  let error = false;

  return {
    writable: new WritableStream<T>({
      write(chunk) {
        (async () => {
          let subStream: ReadableStream<R> | null = null;
          try {
            subStream = callbackFn(chunk);
            subStreamSet.add(subStream);

            for await (const value of subStream) {
              if (error || !subStreamSet.has(subStream)) {
                break;
              }
              readableController.enqueue(value);
            }
          } catch (err: unknown) {
            console.error(err);
          } finally {
            if (subStream) {
              subStreamSet.delete(subStream);
            }

            if (!error && closed && subStreamSet.size === 0) {
              readableController.close();
            }
          }
        })();
      },
      close() {
        closed = true;
      },
      abort(reason) {
        closed = true;
        error = true;
        readableController.error(reason);
        subStreamSet.forEach((stream) => {
          stream.getReader().releaseLock();
          stream.cancel(reason);
        });
        subStreamSet.clear();
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
