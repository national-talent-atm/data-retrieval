import { TextLineStream } from 'jsr:@std/streams/text-line-stream';
import { AsjcData } from './asjc.types.ts';
import { filter, flatToStream, map } from './streams.ts';

const asjcFile = './asjc.txt';
const asjcUrl = new URL(asjcFile, import.meta.url);

export function readAsjcTuple(): ReadableStream<
  readonly [string, string, string, string]
> {
  const body = flatToStream(
    (async () => {
      const body = (await fetch(asjcUrl)).body;

      if (body === null) {
        throw new Error('ASJC body is null.');
      }

      return body;
    })(),
  );

  return body
    .pipeThrough(new TextDecoderStream())
    .pipeThrough(new TextLineStream())
    .pipeThrough(map((value) => value.trim()))
    .pipeThrough(filter((value) => value !== ''))
    .pipeThrough(
      map((value) => value.split('\t', 4) as [string, string, string, string]),
    );
}

export async function readAsjcMap(): Promise<ReadonlyMap<string, AsjcData>> {
  const asjcMap = new Map<string, AsjcData>();

  await readAsjcTuple().pipeTo(
    new WritableStream({
      write([code, ...value]) {
        asjcMap.set(code, {
          'subject-area': value[0],
          'research-branch': value[1],
          'research-field': value[2],
        });
      },
    }),
  );

  return asjcMap;
}

export async function readAsjcJSON(): Promise<{ [code: string]: AsjcData }> {
  return Object.fromEntries(await readAsjcMap());
}
