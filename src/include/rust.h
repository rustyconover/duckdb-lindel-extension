#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>


struct ResultCString {
  enum class Tag {
    Ok,
    Err,
  };

  struct Ok_Body {
    char *_0;
  };

  struct Err_Body {
    char *_0;
  };

  Tag tag;
  union {
    Ok_Body ok;
    Err_Body err;
  };
};


extern "C" {

///Free a value returned from `duckdb_malloc`, `duckdb_value_varchar`, `duckdb_value_blob`, or `duckdb_value_string`.
///
/// ptr: The memory region to de-allocate.
extern void duckdb_free(void *ptr);

///Allocate `size` bytes of memory using the duckdb internal malloc function. Any memory allocated in this manner should be freed using `duckdb_free`.
///
/// size: The number of bytes to allocate.  returns: A pointer to the allocated memory region.
extern void *duckdb_malloc(size_t size);

/// Hash a varchar using the specified hashing algorithm.
ResultCString hashing_varchar(const char *hash_name,
                              size_t hash_name_len,
                              const char *content,
                              size_t len);

/// Create a HMAC using the specified hash function
ResultCString hmac_varchar(const char *hash_name,
                           size_t hash_name_len,
                           const char *key,
                           size_t key_len,
                           const char *content,
                           size_t len);

} // extern "C"
