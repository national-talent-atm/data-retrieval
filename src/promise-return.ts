async function func01() {
  return await Promise.resolve(1);
}

function func02() {
  return Promise.resolve(1);
}

async function func03() {
  return Promise.resolve(1);
}

// function func04() {
//   return await Promise.resolve(1);
// }

async function func05() {
  return 1;
}

await func01();
await func02();
await func03();

await func05();

async function* gen01() {
  yield await Promise.resolve(1);
}

function* gen02() {
  yield Promise.resolve(1);
}

async function* gen03() {
  yield Promise.resolve(1);
}

// function* gen04() {
//   yield await promise.resolve(1);
// }

async function* gen05() {
  yield 1;
}

const source01 = gen01();
const source02 = gen02();
const source03 = gen03();

const source05 = gen05();

await source01.next();
//await source02.next();
await source02.next().value;
await source03.next();

await source05.next();
