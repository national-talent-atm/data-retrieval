import { AdditionalDataFn } from '../extractors/extract-all.ts';

export default (function ({ rest }) {
  const [x_name, x_field, x_university] = rest;

  return {
    x_name,
    x_field,
    x_university,
  };
} as AdditionalDataFn);
