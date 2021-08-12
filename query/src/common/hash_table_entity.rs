pub trait HashTableEntity<Key>: Sized
{
    fn is_zero_key(key: &Key) -> bool;

    unsafe fn is_zero(self: *mut Self) -> bool;
    unsafe fn key_equals(self: *mut Self, key: &Key, hash: u64) -> bool;
    unsafe fn set_key_and_hash(self: *mut Self, key: &Key, hash: u64);

    unsafe fn get_key<'a>(self: *mut Self) -> &'a Key;
    unsafe fn get_hash(self: *mut Self) -> u64;

    unsafe fn not_equals_key(self: *mut Self, other: *mut Self) -> bool;
}

#[repr(C, packed)]
pub struct DefaultHashTableEntity {
    pub(crate) key: i32,
    pub(crate) hash: u64,
}

impl HashTableEntity<i32> for DefaultHashTableEntity
{
    fn is_zero_key(key: &i32) -> bool {
        *key == 0
    }

    unsafe fn is_zero(self: *mut Self) -> bool {
        (*self).key == 0
    }

    unsafe fn key_equals(self: *mut Self, key: &i32, _hash: u64) -> bool {
        (*self).key == *key
    }

    unsafe fn set_key_and_hash(self: *mut Self, key: &i32, hash: u64) {
        (*self).key = *key;
        (*self).hash = hash;
    }

    unsafe fn get_key<'a>(self: *mut Self) -> &'a i32 {
        &(*self).key
    }

    unsafe fn get_hash(self: *mut Self) -> u64 {
        (*self).hash
    }

    unsafe fn not_equals_key(self: *mut Self, other: *mut Self) -> bool {
        (*self).key == (*other).key
    }
}

