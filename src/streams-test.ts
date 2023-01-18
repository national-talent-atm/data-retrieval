import { readableStreamFromIterable } from 'https://deno.land/std@0.171.0/streams/mod.ts';
import { flatMap, mergeMap } from './streams.ts';

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
      // yield new Promise<number>((_, j) => setTimeout(() => j('for test'), 100));
      yield new Promise<number>((r) => setTimeout(() => r(4), 100));
      yield new Promise<number>((r) => setTimeout(() => r(5), 100));
    })(),
  )
    .pipeThrough(
      mergeMap((value) =>
        readableStreamFromIterable(
          (async function* () {
            yield new Promise((r) => setTimeout(() => r(value * 10 + 1), 300));
            yield new Promise((_, j) => j('test-2'));
            yield new Promise((r) => setTimeout(() => r(value * 10 + 2), 200));
            yield new Promise((r) => setTimeout(() => r(value * 10 + 3), 100));
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
