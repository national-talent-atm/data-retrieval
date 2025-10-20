/**
 * Create generator from `Deno.FsFile` that can be transformed
 * to `ReadableStream`.
 *
 * @param reader
 * @param param1
 *
 * @deprecated `Deno.FsFile` has had the `ReadableStream` already.
 */
export async function* readerToAsyncIterable(
  reader: Deno.FsFile,
  { chunkSize = 16_640, closeAfterFinish = false } = {},
) {
  const chunk = new Uint8Array(chunkSize);

  for (
    let read = await reader.read(chunk);
    read !== null;
    read = await reader.read(chunk)
  ) {
    yield chunk.subarray(0, read);
  }

  if (closeAfterFinish) {
    reader.close();
  }
}
