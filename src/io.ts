void (async () => {
  const decoder = new TextDecoder();
  for await (const chunk of Deno.stdin.readable) {
    const text = decoder.decode(chunk);
    console.log(text.trim());
  }
})();

console.log('end');
