# Crypto Hash/HMAC Extension for DuckDB

---

This extension, `crypto`[1], adds cryptographic hash functions and HMAC to DuckDB's SQL engine.  DuckDB already includes a few functions to calculate hash values, but this extension adds additional algorithms and the ability to calculate HMAC codes.  The implementation of the extension is a bit different than normal, it's written in C++ but calls Rust code directly.

[1] - Better naming suggestions

## Hash Digests

```sql
-- Calculate some hash digest values.
 select crypto_hash('sha2-256', 'test');
┌──────────────────────────────────────────────────────────────────┐
│                 crypto_hash('sha2-256', 'test')                  │
│                             varchar                              │
├──────────────────────────────────────────────────────────────────┤
│ 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08 │
└──────────────────────────────────────────────────────────────────┘

select crypto_hash('md5', 'test');
┌──────────────────────────────────┐
│    crypto_hash('md5', 'test')    │
│             varchar              │
├──────────────────────────────────┤
│ 098f6bcd4621d373cade4e832627b4f6 │
└──────────────────────────────────┘
```

### Function Description

`crypto_hash(hash_function_name:VARCHAR, value_to_hash:VARCHAR)`

Calculate the value produced by applying a specified hash function.

The supported hash functions are:

- `blake2b-512`
- `keccak224`
- `keccak256`
- `keccak384`
- `keccak512`
- `md4`
- `md5`
- `sha1`
- `sha2-224`
- `sha2-256`
- `sha2-384`
- `sha2-512`
- `sha3-224`
- `sha3-256`
- `sha3-384`
- `sha3-512`

There result is a lowercase hexadecimal representation of the hash result.

DuckDb already has a function called `hash()` and a function for `sha256()`.

## HMAC calculation

```sql
select crypto_hmac('sha2-256', 'secret key', 'secret message');
┌──────────────────────────────────────────────────────────────────┐
│     crypto_hmac('sha2-256', 'secret key', 'secret message')      │
│                             varchar                              │
├──────────────────────────────────────────────────────────────────┤
│ 2df792e08cefdc0ea9900c67c93cbe66b98231b829a5dccd3857a03baac35963 │
└──────────────────────────────────────────────────────────────────┘
```

`crypto_hmac(hash_function_name:VARCHAR, secret_key:VARCHAR, message:VARCHAR)`

The same list of hash functions that `crypto_hash()` supports are available for use.

### Build Architecture

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

The unique nature of this extension is that it combines a Rust crate (`duckdb_crypto_rust`) with the DuckDB extension template, using a tool called [Corrosion](https://github.com/corrosion-rs/corrosion).  Corrosion handles building the embedded Rust crate as a library, then facilitates the Rust crate being linked link to the DuckDB extension.

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
./build/release/extension/crypto/crypto.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `crypto.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The template contains a single scalar function `crypto()` that takes a string arguments and returns a string:
```
D select crypto_hash('md5', 'test');
┌──────────────────────────────────┐
│    crypto_hash('md5', 'test')    │
│             varchar              │
├──────────────────────────────────┤
│ 098f6bcd4621d373cade4e832627b4f6 │
└──────────────────────────────────┘
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
SET custom_extension_repository='bucket.s3.us-east-1.amazonaws.com/crypto/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL crypto
LOAD crypto
```
