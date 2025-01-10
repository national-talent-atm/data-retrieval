import { readAsjcJSON } from './asjc.ts';

const json = await readAsjcJSON();

console.log(JSON.stringify(json));
