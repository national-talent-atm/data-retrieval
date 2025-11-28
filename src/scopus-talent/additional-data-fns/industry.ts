import { AdditionalDataFn } from '../extractors/extract-all.ts';

export default (function ({ rest }) {
  const [x_industry] = rest;

  return {
    x_industry,
  };
} as AdditionalDataFn);
