export function isCloser(value: unknown): value is { close: () => void } {
  return (
    typeof value === 'object' &&
    value !== null &&
    value !== undefined &&
    'close' in value &&
    // deno-lint-ignore no-explicit-any
    typeof (value as Record<string, any>)['close'] === 'function'
  );
}

export async function* readerToAsyncIterable(
  reader: Deno.FsFile,
  chunkSize = 16_640,
) {
  const chunk = new Uint8Array(chunkSize);

  for (
    let read = await reader.read(chunk);
    read !== null;
    read = await reader.read(chunk)
  ) {
    yield chunk.subarray(0, read);
  }

  if (isCloser(reader)) {
    reader.close();
  }
}
