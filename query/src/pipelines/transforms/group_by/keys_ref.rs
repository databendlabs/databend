use crate::common::HashTableKeyable;

pub struct KeysRef {
    length: usize,
    address: usize,
}

impl Eq for KeysRef {}

impl PartialEq for KeysRef {
    fn eq(&self, other: &Self) -> bool {
        todo!()
    }
}

impl HashTableKeyable for KeysRef {
    fn is_zero(&self) -> bool {
        todo!()
    }

    fn fast_hash(&self) -> u64 {
        todo!()
    }

    fn set_key(&mut self, new_value: &Self) {
        todo!()
    }

    fn eq_with_hash(&self, other_key: &Self) -> bool {
        todo!()
    }
}

