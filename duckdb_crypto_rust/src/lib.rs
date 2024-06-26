use std::ffi::{c_char, c_uchar, c_void, CString};
use std::ptr;
use std::slice;
use std::str;

use digest::{DynDigest, Mac};

use hmac::SimpleHmac;

macro_rules! make_str {
    ( $s : expr , $len : expr ) => {
        unsafe { str::from_utf8_unchecked(slice::from_raw_parts($s as *const u8, $len)) }
    };
}

// Dynamic hash function
fn use_hasher(hasher: &mut dyn DynDigest, data: &[u8]) -> Box<[u8]> {
    hasher.update(data);
    hasher.finalize_reset()
}

// You can use something like this when parsing user input, CLI arguments, etc.
// DynDigest needs to be boxed here, since function return should be sized.
fn select_hasher(s: &str) -> Option<Box<dyn DynDigest>> {
    match s {
        "blake2b-512" => Some(Box::<blake2::Blake2b512>::default()),
        "keccak224" => Some(Box::<sha3::Keccak224>::default()),
        "keccak256" => Some(Box::<sha3::Keccak256>::default()),
        "keccak384" => Some(Box::<sha3::Keccak384>::default()),
        "keccak512" => Some(Box::<sha3::Keccak512>::default()),
        "md4" => Some(Box::<md4::Md4>::default()),
        "md5" => Some(Box::<md5::Md5>::default()),
        "sha1" => Some(Box::<sha1::Sha1>::default()),
        "sha2-224" => Some(Box::<sha2::Sha224>::default()),
        "sha2-256" => Some(Box::<sha2::Sha256>::default()),
        "sha2-384" => Some(Box::<sha2::Sha384>::default()),
        "sha2-512" => Some(Box::<sha2::Sha512>::default()),
        "sha3-224" => Some(Box::<sha3::Sha3_224>::default()),
        "sha3-256" => Some(Box::<sha3::Sha3_256>::default()),
        "sha3-384" => Some(Box::<sha3::Sha3_384>::default()),
        "sha3-512" => Some(Box::<sha3::Sha3_512>::default()),
        _ => None,
    }
}

fn available_hash_algorithms() -> Vec<&'static str> {
    vec![
        "blake2b-512",
        "keccak224",
        "keccak256",
        "keccak384",
        "keccak512",
        "md4",
        "md5",
        "sha1",
        "sha2-224",
        "sha2-256",
        "sha2-384",
        "sha2-512",
        "sha3-224",
        "sha3-256",
        "sha3-384",
        "sha3-512",
    ]
}

#[repr(C)]
pub enum ResultCString {
    Ok(*mut c_char),
    Err(*mut c_char),
}

#[no_mangle]
/// Hash a varchar using the specified hashing algorithm.
pub extern "C" fn hashing_varchar(
    hash_name: *const c_char,
    hash_name_len: usize,

    content: *const c_char,
    len: usize,
) -> ResultCString {
    if hash_name.is_null() || content.is_null() {
        return ResultCString::Ok(ptr::null_mut());
    }

    let hash_name_str = make_str!(hash_name, hash_name_len);
    let content_slice = unsafe { slice::from_raw_parts(content as *const c_uchar, len) };

    match select_hasher(hash_name_str) {
        Some(mut hasher) => {
            let hash_result = use_hasher(&mut *hasher, content_slice);

            // Now hex encode the byte string.
            let hex_encoded = base16ct::lower::encode_string(&hash_result);

            match CString::new(hex_encoded) {
                Ok(c_string) => ResultCString::Ok(c_string.into_raw()),
                Err(_) => ResultCString::Ok(ptr::null_mut()),
            }
        }
        None => {
            let error_message = CString::new(format!(
                "Invalid hash algorithm '{}' available algorithms are: {}",
                hash_name_str,
                available_hash_algorithms().join(", ")
            ))
            .unwrap();
            match CString::new(error_message) {
                Ok(c_string) => ResultCString::Err(c_string.into_raw()),
                Err(_) => ResultCString::Err(ptr::null_mut()),
            }
        }
    }
}

macro_rules! make_hmac {
    ($hash_function : ty, $key: expr, $content: expr) => {
        match SimpleHmac::<$hash_function>::new_from_slice($key).and_then(|mut hmac| {
            hmac.update($content);
            Ok(Box::new(hmac.finalize()))
        }) {
            Ok(final_result) => {
                let hex_encoded =
                    base16ct::lower::encode_string(final_result.into_bytes().as_slice());
                CString::new(hex_encoded)
                    .map(|c_string| ResultCString::Ok(c_string.into_raw()))
                    .unwrap_or(ResultCString::Ok(ptr::null_mut()))
            }
            Err(_) => {
                let error_message = "Failed to create HMAC";
                CString::new(error_message)
                    .map(|c_string| ResultCString::Err(c_string.into_raw()))
                    .unwrap_or(ResultCString::Err(ptr::null_mut()))
            }
        }
    };
}

#[no_mangle]
/// Create a HMAC using the specified hash function and key.
pub extern "C" fn hmac_varchar(
    hash_name: *const c_char,
    hash_name_len: usize,

    key: *const c_char,
    key_len: usize,

    content: *const c_char,
    len: usize,
) -> ResultCString {
    if hash_name.is_null() || content.is_null() {
        return ResultCString::Ok(ptr::null_mut());
    }

    let hash_name_str = make_str!(hash_name, hash_name_len);
    let key_slice = unsafe { slice::from_raw_parts(key as *const c_uchar, key_len) };
    let content_slice = unsafe { slice::from_raw_parts(content as *const c_uchar, len) };

    match hash_name_str {
        "blake2b-512" => {
            make_hmac!(blake2::Blake2b512, key_slice, content_slice)
        }
        "keccak224" => {
            make_hmac!(sha3::Keccak224, key_slice, content_slice)
        }
        "keccak256" => {
            make_hmac!(sha3::Keccak256, key_slice, content_slice)
        }
        "keccak384" => {
            make_hmac!(sha3::Keccak384, key_slice, content_slice)
        }
        "keccak512" => {
            make_hmac!(sha3::Keccak512, key_slice, content_slice)
        }
        "md4" => {
            make_hmac!(md4::Md4, key_slice, content_slice)
        }
        "md5" => {
            make_hmac!(md5::Md5, key_slice, content_slice)
        }
        "sha1" => {
            make_hmac!(sha1::Sha1, key_slice, content_slice)
        }
        "sha2-224" => {
            make_hmac!(sha2::Sha224, key_slice, content_slice)
        }
        "sha2-256" => {
            make_hmac!(sha2::Sha256, key_slice, content_slice)
        }
        "sha2-384" => {
            make_hmac!(sha2::Sha384, key_slice, content_slice)
        }
        "sha2-512" => {
            make_hmac!(sha2::Sha512, key_slice, content_slice)
        }
        "sha3-224" => {
            make_hmac!(sha3::Sha3_224, key_slice, content_slice)
        }
        "sha3-256" => {
            make_hmac!(sha3::Sha3_256, key_slice, content_slice)
        }
        "sha3-384" => {
            make_hmac!(sha3::Sha3_384, key_slice, content_slice)
        }
        "sha3-512" => {
            make_hmac!(sha3::Sha3_512, key_slice, content_slice)
        }
        _ => {
            let error_message = CString::new(format!(
                "Invalid hash algorithm '{}' available algorithms are: {}",
                hash_name_str,
                available_hash_algorithms().join(", ")
            ))
            .unwrap();
            match CString::new(error_message) {
                Ok(c_string) => ResultCString::Err(c_string.into_raw()),
                Err(_) => ResultCString::Err(ptr::null_mut()),
            }
        }
    }
}

#[cfg(test)]
mod tests {}

// Setup the global allocator to use the duckdb internal malloc and free functions.
extern "C" {
    #[doc = "Allocate `size` bytes of memory using the duckdb internal malloc function. Any memory allocated in this manner\nshould be freed using `duckdb_free`.\n\n size: The number of bytes to allocate.\n returns: A pointer to the allocated memory region."]
    pub fn duckdb_malloc(size: usize) -> *mut ::std::os::raw::c_void;
}
extern "C" {
    #[doc = "Free a value returned from `duckdb_malloc`, `duckdb_value_varchar`, `duckdb_value_blob`, or\n`duckdb_value_string`.\n\n ptr: The memory region to de-allocate."]
    pub fn duckdb_free(ptr: *mut ::std::os::raw::c_void);
}

use std::alloc::{GlobalAlloc, Layout};

// Implement a Rust allocator that calls duckdb_malloc and duckdb_free and use it as the global allocator.
struct DuckDBAllocator {}

impl DuckDBAllocator {
    const fn new() -> Self {
        DuckDBAllocator {}
    }
}

unsafe impl GlobalAlloc for DuckDBAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        duckdb_malloc(layout.size()) as *mut u8
    }

    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        duckdb_free(ptr as *mut c_void);
    }
}

#[global_allocator]
static A: DuckDBAllocator = DuckDBAllocator::new();
