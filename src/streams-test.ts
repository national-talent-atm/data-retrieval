import { readableStreamFromIterable } from 'https://deno.land/std@0.171.0/streams/mod.ts';
import { flatMap, mergeMap, switchMap } from './streams.ts';

async function* gen01() {
  const items = [1, 2, 3, 4, 5];
  for (const value of items) {
    console.debug('generator', value);
    yield value;
  }
}

const source = gen01();

// console.debug(source.next());
// console.debug(source.next());
// console.debug(source.next());
// console.debug(source.next());

const steam = new ReadableStream<number>(
  {
    async pull(controller) {
      const { value, done } = await source.next();
      if (done) {
        controller.close();
      } else {
        console.debug('pull', value);
        controller.enqueue(value);
      }
    },
  },
  new CountQueuingStrategy({ highWaterMark: 0 }),
);

await steam.pipeTo(
  new WritableStream<number>({
    write(chunk): void {
      console.info('output', chunk);
    },
  }),
);

await readableStreamFromIterable([1, 2, 3, 4])
  .pipeThrough(
    flatMap((value) =>
      readableStreamFromIterable(
        (async function* () {
          yield await new Promise((r) =>
            setTimeout(() => r(value * 10 + 1), 100),
          );
          yield await new Promise((r) =>
            setTimeout(() => r(value * 10 + 2), 100),
          );
          yield await new Promise((r) =>
            setTimeout(() => r(value * 10 + 3), 100),
          );
        })(),
      ),
    ),
  )
  .pipeTo(
    new WritableStream({
      write(value) {
        console.info('pipe-test', value);
      },
    }),
  );

try {
  await readableStreamFromIterable(
    (async function* () {
      yield new Promise<number>((r) => setTimeout(() => r(1), 100));
      yield new Promise<number>((r) => setTimeout(() => r(2), 100));
      yield new Promise<number>((r) => setTimeout(() => r(3), 100));
      yield new Promise<number>((_, j) => setTimeout(() => j('for test'), 100));
      yield new Promise<number>((r) => setTimeout(() => r(4), 100));
      yield new Promise<number>((r) => setTimeout(() => r(5), 100));
    })(),
  )
    .pipeThrough(
      mergeMap((value) =>
        readableStreamFromIterable(
          (async function* () {
            yield new Promise<number>((r) =>
              setTimeout(() => r(value * 10 + 1), 300),
            );
            //yield new Promise<number>((_, j) => j('test-2'));
            yield new Promise<number>((r) =>
              setTimeout(() => r(value * 10 + 2), 200),
            );
            yield new Promise<number>((r) =>
              setTimeout(() => r(value * 10 + 3), 100),
            );
          })(),
        ),
      ),
    )
    .pipeTo(
      new WritableStream({
        write(value) {
          console.info('merge-test', value);
        },
      }),
    );
} catch (err: unknown) {
  console.error('main', err);
}

try {
  await readableStreamFromIterable(
    (async function* () {
      yield new Promise<number>((r) => {
        console.debug('source:', 1);
        setTimeout(() => r(1), 200);
      });
      yield new Promise<number>((r) => {
        console.debug('source:', 2);
        setTimeout(() => r(2), 300);
      });
      yield new Promise<number>((r) => {
        console.debug('source:', 3);
        setTimeout(() => r(3), 400);
      });
      yield new Promise<number>((_, j) => setTimeout(() => j('for test'), 100));
      yield new Promise<number>((r) => {
        console.debug('source:', 4);
        setTimeout(() => r(4), 500);
      });
      yield new Promise<number>((r) => {
        console.debug('source:', 5);
        setTimeout(() => r(5), 600);
      });
    })(),
  )
    .pipeThrough(
      switchMap((value) =>
        readableStreamFromIterable(
          (async function* () {
            yield new Promise<number>((r) =>
              setTimeout(() => r(value * 10 + 1), 100),
            );
            yield new Promise<number>((_, j) => j('test-2'));
            yield new Promise<number>((r) =>
              setTimeout(() => r(value * 10 + 2), 200),
            );
            yield new Promise<number>((r) =>
              setTimeout(() => r(value * 10 + 3), 300),
            );
          })(),
        ),
      ),
    )
    .pipeTo(
      new WritableStream({
        write(value) {
          console.info('switch-test', value);
        },
      }),
    );
} catch (err: unknown) {
  console.error('main', err);
}

function testTransformStream<I>(
  writableStrategy?: QueuingStrategy<I>,
  readableStrategy?: QueuingStrategy<I>,
): TransformStream<I, I> {
  const { writable, readable } = new TransformStream<I, I>(
    undefined,
    writableStrategy,
  );

  const iterator = readable[Symbol.asyncIterator]();

  let closed = false;

  return {
    writable,
    readable: new ReadableStream<I>(
      {
        async pull(controller) {
          console.debug('--- in pull');
          try {
            iterator.next().then((result) => {
              if (result.done) {
                if (!closed) {
                  closed = true;
                  controller.close();
                }
                return;
              }
              console.debug('resolved:', result.value);
              controller.enqueue(result.value);
            });
          } catch (error) {
            // Propagate error to stream from iterator
            // If the stream status is "errored", it will be thrown, but ignore.
            await readable.cancel(error).catch(() => {});
            controller.error(error);
            return;
          }
        },
        async cancel(reason) {
          // Propagate cancellation to readable and iterator
          if (typeof iterator.throw == 'function') {
            try {
              await iterator.throw(reason);
            } catch {
              /* `iterator.throw()` always throws on site. We catch it. */
            }
          }
          await readable.cancel(reason);
        },
      },
      readableStrategy,
    ),
  };
}

try {
  await readableStreamFromIterable(
    (async function* () {
      yield new Promise<number>((r) => {
        console.debug('source:', 1);
        setTimeout(() => r(1), 8000);
      });
      yield new Promise<number>((r) => {
        console.debug('source:', 2);
        setTimeout(() => r(2), 6000);
      });
      yield new Promise<number>((r) => {
        console.debug('source:', 3);
        setTimeout(() => r(3), 10000);
      });
      yield new Promise<number>((r) => {
        console.debug('source:', 4);
        setTimeout(() => r(4), 0);
      });
      yield new Promise<number>((r) => {
        console.debug('source:', 5);
        setTimeout(() => r(5), 0);
      });
    })(),
  )
    .pipeThrough(
      testTransformStream(
        new CountQueuingStrategy({ highWaterMark: 10 }),
        new CountQueuingStrategy({ highWaterMark: 10 }),
      ),
    )
    .pipeTo(
      new WritableStream({
        write(value) {
          console.info('non-blocking-test', value);
        },
      }),
    );
} catch (err) {
  console.error(err);
}
