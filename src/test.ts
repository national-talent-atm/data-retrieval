import {
  readableStreamFromReader,
  TextLineStream,
} from 'https://deno.land/std@0.171.0/streams/mod.ts';
import { filter, map } from './streams.ts';

//const apiKey = '8cb7c1b4f922a98a843db4eed58359d5';

//const url = 'https://api.elsevier.com/content/author/author_id';

const inputFile = './target/test.txt';

const stream = readableStreamFromReader(
  await Deno.open(inputFile, { read: true }),
)
  .pipeThrough(new TextDecoderStream())
  .pipeThrough(new TextLineStream())
  .pipeThrough(map((value) => value.trim()))
  .pipeThrough(filter((value) => value !== ''));

stream.pipeTo(
  new WritableStream({
    write(line) {
      console.info('line:', line);
    },
  }),
);

// (async () => {
//   try {
//     const res = await fetch(`${url}/7403147159`, {
//       headers: {
//         Accept: 'application/json',
//         'X-ELS-APIKey': apiKey,
//       },
//     });
//     if (res.status !== 200) {
//       throw new Error(await res.text());
//     }
//     console.info(await res.text());
//   } catch (err: unknown) {
//     console.error(err);
//   }
// })();
