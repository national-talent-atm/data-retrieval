function* gen01() {
  console.debug('geneate:', 1);
  yield 1;
  console.debug('geneate:', 2);
  yield 2;
  console.debug('geneate:', 3);
  yield 3;
}

//[...gen01()].map((value) => console.info('result:', value));

for (const value of gen01()) {
  console.info('result:', value);
}

let dNumber!: number;
let dString!: string;
let dNumberOrString!: number | string;

const myTest01 = (x: number | string): number | string => {
  return x;
};

const test01_01 = myTest01(dNumber);
const test01_02 = myTest01(dString);
const test01_03 = myTest01(dNumberOrString);

const myTest02 = <T extends number | string>(x: T): T => {
  return x;
};

const test02_01 = myTest02(dNumber);
const test02_02 = myTest02(dString);
const test02_03 = myTest02(dNumberOrString);

const myTest03 = (
  x: number | string,
): typeof x extends number | string
  ? number | string
  : typeof x extends number
  ? number
  : string => {
  return x;
};

const test03_01 = myTest03(dNumber);
const test03_02 = myTest03(dString);
const test03_03 = myTest03(dNumberOrString);

const myTest04 = <T extends number | string>(
  x: T,
): typeof x extends number | string
  ? number | string
  : typeof x extends number
  ? number
  : string => {
  throw 'unimplemented';
};

const test04_01 = myTest04(dNumber);
const test04_02 = myTest04(dString);
const test04_03 = myTest04(dNumberOrString);

const myTest90 = <T extends number | string>(
  x: T,
): T extends number ? 'a' : 'b' => {
  throw 'unimplmented';
};

const test90_01 = myTest90(dNumber);
const test90_02 = myTest90(dString);
const test90_03 = myTest90(dNumberOrString);
