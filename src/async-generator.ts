let label = '';

function createTimeout<T>(value: T, ms: number) {
  return (r: (value: T | PromiseLike<T>) => void) => {
    console.timeLog(label, 'start solving:', value);
    setTimeout(() => {
      console.timeLog(label, 'internal-resolved:', value);
      r(value);
    }, ms);
  };
}

async function* gen01() {
  yield await new Promise<number>(createTimeout(1, 1500));
  yield await new Promise<number>(createTimeout(2, 1000));
  yield await new Promise<number>(createTimeout(3, 500));
}

async function* gen02() {
  yield new Promise<number>(createTimeout(1, 1500));
  yield new Promise<number>(createTimeout(2, 1000));
  yield new Promise<number>(createTimeout(3, 500));
}

function* gen03() {
  yield new Promise<number>(createTimeout(1, 1500));
  yield new Promise<number>(createTimeout(2, 1000));
  yield new Promise<number>(createTimeout(3, 500));
}

label = 'run-gen01-promise';
console.time(label);
await(async () => {
  const source = gen01();

  return await Promise.all([
    source.next().then((result) => console.timeLog(label, result)),
    source.next().then((result) => console.timeLog(label, result)),
    source.next().then((result) => console.timeLog(label, result)),
  ]);
})();
console.timeEnd(label);
console.log('-'.repeat(80));

label = 'run-gen01-await';
console.time(label);
await(async () => {
  const source = gen01();

  console.timeLog(label, await source.next());
  console.timeLog(label, await source.next());
  console.timeLog(label, await source.next());
})();
console.timeEnd(label);
console.log('-'.repeat(80));

label = 'run-gen02-promise';
console.time(label);
await(() => {
  const source = gen02();

  return Promise.all([
    source.next().then((result) => console.timeLog(label, result)),
    source.next().then((result) => console.timeLog(label, result)),
    source.next().then((result) => console.timeLog(label, result)),
  ]);
})();
console.timeEnd(label);
console.log('-'.repeat(80));

label = 'run-gen02-await';
console.time(label);
await(async () => {
  const source = gen02();

  console.timeLog(label, await source.next());
  console.timeLog(label, await source.next());
  console.timeLog(label, await source.next());
})();
console.timeEnd(label);
console.log('-'.repeat(80));

label = 'run-gen03-promise';
console.time(label);
await(() => {
  const source = gen03();

  return Promise.all([
    source.next().value?.then((result) => console.timeLog(label, result)),
    source.next().value?.then((result) => console.timeLog(label, result)),
    source.next().value?.then((result) => console.timeLog(label, result)),
  ]);
})();
console.timeEnd(label);
console.log('-'.repeat(80));

label = 'run-gen03-await';
console.time(label);
await(async () => {
  const source = gen03();

  console.timeLog(label, await source.next().value);
  console.timeLog(label, await source.next().value);
  console.timeLog(label, await source.next().value);
})();
console.timeEnd(label);
console.log('-'.repeat(80));
