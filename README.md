# Installation

Download and install [Deno](https://deno.com/).

Clone GitHub repository.

```bash
$ git clone https://github.com/national-talent-atm/data-retrieval.git
```

# System Configuration

Change directory to `data-retrieval`.

```bash
cd data-retrieval
```

Create file `.env` with the following variable:

```text
ELSEVIER_KEY=Your_API_key,Your_other_API_keys
```

Get Elsevier API Key form [https://dev.elsevier.com/](https://dev.elsevier.com/).

# Searching for Scopus IDs

If you **don't have Scopus ID** but you have firstname and lastname of authors, then go for this section.
If you already have had Scopus ID for authors, then go to [Retrieval Authors Data](#retrieval-authors-data).

## Running Configuration

Create directory `target` in project root direcotry.
Create a **non-header _tab-separated-value_** file with extension `.txt` in directory `target`, e.g., `target/my-search-data.txt`.
The file name **without `target` directory and extension** is a _configuration name_, e.g., `my-search-data`.

### First column (search name)

The first column is a search data that consists of the list of posible firstname and lastname separated by semi-colon (`;`), e.g., `dian smith;dian simson` where `smith` is the new lastname and `simson` is the old lastname.

### Second and Third column (firstname and lastname)

This 2-columns are used to be reference in the result.

### Fourth column (industry)

This will be included in the result.

**Example file:** [`my-search-data.txt`](./examples/my-search-data.txt).

## Running Program

For running program, your organization **must** subscribe for Scopus service and you **must** connect from your organization network.
Call `src/scopus-talent/scripts/get-id-by-names.ts` script with _configuration name_.

```bash
$ deno run --env-file ./src/scopus-talent/scripts/get-id-by-names.ts my-search-data
```

Accept all permissions.

## Result

The result is a **_comma-separated-value_** (`.csv`) file stored in directory `target/output/configuration_name/configuration_name-result.csv`.
It consists of the posible Scopus IDs for each input row.

**Example file:** [`my-search-data-result.csv`](./examples/my-search-data-result.csv).

# Retrieval Authors Data

## Running Configuration

Create directory `target` in project root direcotry.
Create a **non-header _tab-separated-value_** file with extension `.txt` in directory `target`, e.g., `target/my-scopus-id-data.txt`.
The file name **without `target` directory and extension** is a _configuration name_, e.g., `my-scopus-id-data`.
You can use the result from [Searching for Scopus IDs](#searching-for-scopus-ids) to create this file.

The first column, **_Scopus ID_**, is **only required**.
The _industry_ column may be required if you want to import to ATM database.

**Example file:** [`my-scopus-id-data.txt`](./examples/my-scopus-id-data.txt).

## Running Program

For running program, your organization **must** subscribe for Scopus service and you **must** connect from your organization network.
Call `src/scopus-talent/scripts/get-by-id.ts` script with _configuration name_.

```bash
$ deno run --env-file ./src/scopus-talent/scripts/get-by-id.ts my-scopus-id-data
```

If _industry_ is the second column of your input file, then you can use `--additional-data-fn` to extract this information by using provided helper function `./src/scopus-talent/additional-data-fns/industry.ts`.

```bash
$ deno run --env-file ./src/scopus-talent/scripts/get-by-id.ts \
    --additional-data-fn=$(realpath ./src/scopus-talent/additional-data-fns/industry.ts) \
    my-scopus-id-data
```

Accept all permissions.

You can use [`industry.ts`](./src/scopus-talent/additional-data-fns/industry.ts) or [`name-field-university.ts`](./src/scopus-talent/additional-data-fns/name-field-university.ts) to be examples for your extractor function.

## Result

The result is a **_comma-separated-value_** (`.csv`) file stored in directory `target/output/configuration_name/configuration_name-result.csv`.
It consists of the most required data for ATM database.

**Example file:** [`my-scopus-id-data-result.csv`](./examples/my-scopus-id-data-result.csv).
