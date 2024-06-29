# Lindel (lineariser-delineariser) Extension for DuckDB

---

This extension, `lindel`[1], adds functions that perform linearisation and delinearisation for arrays of values in DuckDB's SQL engine.

## What is linearisation?

Linearization refers to the process of mapping multi-dimensional data into a one-dimensional sequence while preserving locality. This is often used to improve the efficiency of data structures and algorithms that deal with spatial data, such as in databases, geographic information systems (GIS), and memory caches.

> "The principle of locality states that programs tend to reuse data and instructions they have used recently."

In SQL you are often sorting by a time column or by unique identifier and often that is all that is necessary to handle the query patterns of the data. But sometimes the data is queried by more than one column/field, some examples of this that are known to the author are:

- Queries by time and identifier (historical trading data across baskets of securities)
- Latitude and Longitude (GIS applications)
- Latitude, Longitude and Altitude (tracking flights)
- Latitude, Longitude, Altitude, Time (tracking flight history over areas)

Just sorting a table by a single field's value doesn't lead to optimal query performance.  If you sort just by latitude, and you query by longitude you're going to read the entire table to process that query.

Linearisation can take multiple field values and map all of those values into to single numeric value, while preserving the locality of the mapped values.  By preserving locality it means that values that were considered "close" in the unmapped representation are still numerically close in the mapped representation.

## When would I use this?

When you have a query pattern across multiple numeric or short text columns or fields and you are using Parquet you should consider sorting rows using Hilbert encoding.

Often when I'm producing data that will be stored in Parquet I'd store the data using this ordering:

```sql
COPY (
  select * from 'source.csv'
  order by
  hilbert_encode([source_data.time, source_data.symbol_id]::integer[2])
)
TO 'example.parquet' (FORMAT PARQUET)

-- or if dealing with latitude and longitude

COPY (
  select * from 'source.csv'
  order by
  hilbert_encode([source_data.lat, source_data.long]::double[2])
) TO 'example.parquet' (FORMAT PARQUET)
```

## Encoding Types

This extension offers two different encoding types, Hilbert and Morton encoding.

### Hilbert Encoding

Hilbert encoding uses the Hilbert curve, a continuous fractal space-filling curve named after mathematician David Hilbert. Unlike Morton encoding, which uses bit interleaving, Hilbert encoding rearranges the coordinates based on the Hilbert curve’s path.

### Morton Encoding (Z-order Curve)

Morton encoding, also known as the Z-order curve, is a method of interleaving the binary representations of multiple coordinates into a single integer. It derives its name from the mathematician and computer scientist Glenn K. Morton.

**Locality:** Hilbert encoding generally preserves locality better than Morton encoding, making it preferable for applications where spatial proximity matters.

## API

The types that can be encoded are any signed or unsigned integer type along with any float or double.  This is referred to as an `INPUT_TYPE`.

The output will be the smallest unsigned integer type that can represent the data type and number of values passed in the input array.

### Encoding Functions

* `hilbert_encode(ARRAY[INPUT_TYPE, 1-16])`
* `morton_encode(ARRAY[INPUT_TYPE, 1-16])`

Since the largest value that can be returned by this function is a 128-bit `UHUGEINT`, there are restrictions about the maximum number of elements that can be encoded.  The input array size is validated to ensure that all input elements will be contained in the up to 128-bit output.

| INPUT TYPE | Maximum Number of Elements | Output Type (depends on number of elements) |
|---|--|-------------|
| `UTINYINT`   | 16 | 1: `UTINYINT`<br/>2: `USMALLINT`<br/>3-4: `UINTEGER`<br/> 4-8: `UBIGINT`<br/> 8-16: `UHUGEINT`|
| `USMALLINT`  | 8 | 1: `USMALLINT`<br/>2: `UINTEGER`<br/>3-4: `UBIGINT`<br/>4-8: `UHUGEINT` |
| `UINTEGER`   | 4 | 1: `UINTEGER`<br/>2: `UBIGINT`<br/>3-4: `UHUGEINT` |
| `UBIGINT`    | 2 | 1: `UBIGINT`<br/>2: `UHUGEINT` |
| `FLOAT`      | 4 | 1: `UINTEGER`<br/>2: `UBIGINT`<br/>3-4: `UHUGEINT` |
| `DOUBLE`     | 2 | 1: `UBIGINT`<br/>2: `UHUGEINT` |

### Encoding examples

```sql
with elements as (
  select * as id from range(3)
)
select
  a.id as a,
  b.id as b,
  hilbert_encode([a.id, b.id]::tinyint[2]) as hilbert,
  morton_encode([a.id, b.id]::tinyint[2]) as morton
  from
elements as a cross join elements as b;
┌───────┬───────┬─────────┬────────┐
│   a   │   b   │ hilbert │ morton │
│ int64 │ int64 │ uint16  │ uint16 │
├───────┼───────┼─────────┼────────┤
│     0 │     0 │       0 │      0 │
│     0 │     1 │       3 │      1 │
│     0 │     2 │       4 │      4 │
│     1 │     0 │       1 │      2 │
│     1 │     1 │       2 │      3 │
│     1 │     2 │       7 │      6 │
│     2 │     0 │      14 │      8 │
│     2 │     1 │      13 │      9 │
│     2 │     2 │       8 │     12 │
└───────┴───────┴─────────┴────────┘

-- Now sort that same table using Hilbert encoding

┌───────┬───────┬─────────┬────────┐
│   a   │   b   │ hilbert │ morton │
│ int64 │ int64 │ uint16  │ uint16 │
├───────┼───────┼─────────┼────────┤
│     0 │     0 │       0 │      0 │
│     1 │     0 │       1 │      2 │
│     1 │     1 │       2 │      3 │
│     0 │     1 │       3 │      1 │
│     0 │     2 │       4 │      4 │
│     1 │     2 │       7 │      6 │
│     2 │     2 │       8 │     12 │
│     2 │     1 │      13 │      9 │
│     2 │     0 │      14 │      8 │
└───────┴───────┴─────────┴────────┘

-- Do you notice how when A and B are closer to 2 the rows are "closer"?
```

Encoding doesn't only work with integers it can also be used with floats.

```sql
-- Encode two 32-bit floats into one uint64
select hilbert_encode([37.8, .2]::float[2]);
┌─────────────────────────────────────────────────────────────┐
│ hilbert_encode(CAST(main.list_value(37.8, .2) AS FLOAT[2])) │
│                           uint64                            │
├─────────────────────────────────────────────────────────────┤
│                                         2303654869236839926 │
└─────────────────────────────────────────────────────────────┘

-- Since doubles use 64 bits of precision the encoding
-- must result in a uint128

 select hilbert_encode([37.8, .2]::double[2]);
┌──────────────────────────────────────────────────────────────┐
│ hilbert_encode(CAST(main.list_value(37.8, .2) AS DOUBLE[2])) │
│                           uint128                            │
├──────────────────────────────────────────────────────────────┤
│                       42534209309512799991913666633619307890 │
└──────────────────────────────────────────────────────────────┘

-- 3 dimensional encoding.
select hilbert_encode([1.0, 5.0, 6.0]::float[3]);
┌──────────────────────────────────────────────────────────────────┐
│ hilbert_encode(CAST(main.list_value(1.0, 5.0, 6.0) AS FLOAT[3])) │
│                             uint128                              │
├──────────────────────────────────────────────────────────────────┤
│                                     8002395622101954260073409974 │
└──────────────────────────────────────────────────────────────────┘
```

Not to be left out you can also encode strings.

```sql

select hilbert_encode([ord(x) for x in split('abcd', '')]::tinyint[4]);
┌───────────────────────────────────────────────────────────────────────────────────────┐
│ hilbert_encode(CAST(main.list_apply(split('abcd', ''), (x -> ord(x))) AS TINYINT[4])) │
│                                        uint32                                         │
├───────────────────────────────────────────────────────────────────────────────────────┤
│                                                                             178258816 │
└───────────────────────────────────────────────────────────────────────────────────────┘

--- This splits the string 'abcd' by character, then converts each character into
--- its ordinal representation, finally converts them all to 8 bit integers and then
--- performs encoding.

```

Currently, the input of `hilbert_encode()` and `morton_encode()` is that the input
DuckDB arrays, which means each array element consists of the same size.  If you want to encode
different sized types it is possible, but you need to always break up the larger data types
into the units of the smallest data type.  **Your mileage may vary.**

### Decoding Functions

* `hilbert_encode(ANY_UNSIGNED_INTEGER, TINYINT, BOOLEAN, BOOLEAN)`
* `morton_encode(ANY_UNSIGNED_INTEGER, TINYINT, BOOLEAN, BOOLEAN)`

The decoding functions take three parameters:

The first parameter is the value to be decoded, this is always an unsigned integer type.

The second parameter is the number of elements that should be decoded from the value.

The third parameter is a boolean that indicates that the values should be returned as floats (either `REAL` or `DOUBLE`) if set to true.

The fourth parameter is a boolean that indicates that the values should be unsinged if not using floats.

The actual return types of these functions will always be an array where the type of the element of the array is determined by the number of elements requested and if "float" handling is enabled by the third parameter.

### Examples

```sql
-- Start out just by encoding two values.
select hilbert_encode([1, 2]::tinyint[2]) as hilbert;
┌─────────┐
│ hilbert │
│ uint16  │
├─────────┤
│       7 │
└─────────┘
D select hilbert_decode(7::uint16, 2, false, true) as values;
┌─────────────┐
│   values    │
│ utinyint[2] │
├─────────────┤
│ [1, 2]      │
└─────────────┘

-- Show that the decoder works with the encoder.
select hilbert_decode(hilbert_encode([1, 2]::tinyint[2]), 2, false, false) as values;
┌─────────────┐
│   values    │
│ utinyint[2] │
├─────────────┤
│ [1, 2]      │
└─────────────┘

-- FIXME: need to implement a signed or unsigned flag on the decoder function.
select hilbert_decode(hilbert_encode([1, -2]::bigint[2]), 2, false, false) as values;
┌───────────┐
│  values   │
│ bigint[2] │
├───────────┤
│ [1, -2]   │
└───────────┘

select hilbert_encode([1.0, 5.0, 6.0]::float[3]) as hilbert;
┌──────────────────────────────┐
│           hilbert            │
│           uint128            │
├──────────────────────────────┤
│ 8002395622101954260073409974 │
└──────────────────────────────┘

select hilbert_decode(8002395622101954260073409974::UHUGEINT, 3, True, False) as values;
┌─────────────────┐
│     values      │
│    float[3]     │
├─────────────────┤
│ [1.0, 5.0, 6.0] │
└─────────────────┘
```

### Build Architecture

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

The unique nature of this extension is that it combines a Rust crate (`duckdb_lindel_rust`) with the DuckDB extension template, using a tool called [Corrosion](https://github.com/corrosion-rs/corrosion).  Corrosion handles building the embedded Rust crate as a library, then facilitates the Rust crate being linked link to the DuckDB extension.

For the DuckDB extension to call the Rust code a tool called `cbindgen` is used to write the C++ headers for the exposed Rust interface.

The headers can be updated by running `make rust_binding_headers`.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/lindel/lindel.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `lindel.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB.

```
D select hilbert_encode([1.0, 5.0, 6.0]::float[3]) as hilbert;
┌──────────────────────────────┐
│           hilbert            │
│           uint128            │
├──────────────────────────────┤
│ 8002395622101954260073409974 │
└──────────────────────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.us-east-1.amazonaws.com/lindel/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL lindel
LOAD lindel
```
