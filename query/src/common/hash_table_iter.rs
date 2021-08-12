use std::marker::PhantomData;
use crate::common::hash_table_entity::HashTableEntity;

pub struct HashTableIter<Key, Entity: HashTableEntity<Key>> {
    idx: isize,
    size: isize,
    capacity: isize,
    entities: *mut Entity,
    zero_entity: Option<*mut Entity>,

    phantom: PhantomData<Key>,

}

impl<Key, Entity: HashTableEntity<Key>> HashTableIter<Key, Entity> {
    pub fn new(size: isize, capacity: isize, entities: *mut Entity, zero_entity: Option<*mut Entity>) -> impl Iterator<Item=*mut Entity> {
        Self {
            idx: -2,
            size,
            capacity,
            entities,
            zero_entity,
            phantom: PhantomData::default(),
        }
    }
}

impl<Key, Entity: HashTableEntity<Key>> Iterator for HashTableIter<Key, Entity> {
    type Item = *mut Entity;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            if self.idx == -2 {
                self.idx = -1;
                if self.zero_entity.is_some() {
                    return self.zero_entity;
                }
            }

            self.idx += 1;
            while self.idx < self.capacity && self.entities.offset(self.idx).is_zero() {
                self.idx += 1;
            }

            match self.idx == self.capacity {
                true => None,
                false => Some(self.entities.offset(self.idx))
            }
        }
    }
}
