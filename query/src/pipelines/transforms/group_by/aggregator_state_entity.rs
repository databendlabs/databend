use crate::common::HashTableEntity;
use crate::common::HashTableKeyable;
use crate::common::KeyValueEntity;

pub trait StateEntity<Key> {
    fn set_state_key(self: *mut Self, key: &Key);
    fn get_state_key<'a>(self: *mut Self) -> &'a Key;
    fn set_state_value(self: *mut Self, value: usize);
    fn get_state_value<'a>(self: *mut Self) -> &'a usize;
}

pub trait ShortFixedKeyable: Sized + Clone {
    fn lookup(&self) -> isize;
    fn is_zero_key(&self) -> bool;
}

pub struct ShortFixedKeysStateEntity<Key: ShortFixedKeyable> {
    key: Key,
    value: usize,
}

impl<Key: ShortFixedKeyable> StateEntity<Key> for ShortFixedKeysStateEntity<Key> {
    #[inline(always)]
    fn set_state_key(self: *mut Self, key: &Key) {
        unsafe { (*self).key = key.clone() }
    }

    #[inline(always)]
    fn get_state_key<'a>(self: *mut Self) -> &'a Key {
        unsafe { &(*self).key }
    }

    #[inline(always)]
    fn set_state_value(self: *mut Self, value: usize) {
        unsafe { (*self).value = value }
    }

    #[inline(always)]
    fn get_state_value<'a>(self: *mut Self) -> &'a usize {
        unsafe { &(*self).value }
    }
}

impl<Key: HashTableKeyable> StateEntity<Key> for KeyValueEntity<Key, usize> {
    fn set_state_key(self: *mut Self, _key: &Key) {
        unimplemented!()
    }

    #[inline(always)]
    fn get_state_key<'a>(self: *mut Self) -> &'a Key {
        self.get_key()
    }

    #[inline(always)]
    fn set_state_value(self: *mut Self, value: usize) {
        self.set_value(value)
    }

    #[inline(always)]
    fn get_state_value<'a>(self: *mut Self) -> &'a usize {
        self.get_value()
    }
}

impl ShortFixedKeyable for u8 {
    #[inline(always)]
    fn lookup(&self) -> isize {
        *self as isize
    }

    #[inline(always)]
    fn is_zero_key(&self) -> bool {
        *self == 0
    }
}

impl ShortFixedKeyable for u16 {
    #[inline(always)]
    fn lookup(&self) -> isize {
        *self as isize
    }

    #[inline(always)]
    fn is_zero_key(&self) -> bool {
        *self == 0
    }
}
