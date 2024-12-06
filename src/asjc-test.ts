import { readAsjcMap } from './asjc.ts';

const asjcMap = await readAsjcMap();

console.debug(asjcMap);
