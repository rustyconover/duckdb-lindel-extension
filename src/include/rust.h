#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>



extern "C" {

///Free a value returned from `duckdb_malloc`, `duckdb_value_varchar`, `duckdb_value_blob`, or `duckdb_value_string`.
///
/// ptr: The memory region to de-allocate.
extern void duckdb_free(void *ptr);

///Allocate `size` bytes of memory using the duckdb internal malloc function. Any memory allocated in this manner should be freed using `duckdb_free`.
///
/// size: The number of bytes to allocate.  returns: A pointer to the allocated memory region.
extern void *duckdb_malloc(size_t size);

void hilbert_encode_u16_var(const uint16_t *ptr, size_t len, void *result);

void hilbert_encode_u32_var(const uint32_t *ptr, size_t len, void *result);

void hilbert_encode_u64_var(const uint64_t *ptr, size_t len, void *result);

void hilbert_encode_u8_var(const uint8_t *ptr, size_t len, void *result);

void morton_encode_u16_var(const uint16_t *ptr, size_t len, void *result);

void morton_encode_u32_var(const uint32_t *ptr, size_t len, void *result);

void morton_encode_u64_var(const uint64_t *ptr, size_t len, void *result);

void morton_encode_u8_var(const uint8_t *ptr, size_t len, void *result);

void perform_decode(uint8_t encoding_type,
                    uint8_t element_bit_width,
                    const void *src,
                    void *dest,
                    size_t dest_len);

} // extern "C"
