use std::marker::PhantomData;
use crate::common::hash_table_entity::HashTableEntity;

pub struct HashTableIter<Key, Entity: HashTableEntity<Key>> {
    idx: isize,
    size: isize,
    entities: *mut Entity,
    zero_entity: Option<*mut Entity>,
    ss: PhantomData<Key>,

}

impl<Key, Entity: HashTableEntity<Key>> HashTableIter<Key, Entity> {
    pub fn new(size: isize, entities: *mut Entity, zero_entity: Option<*mut Entity>) -> Self {
        Self {
            idx: 0,
            size,
            entities,
            zero_entity,
            ss: PhantomData::default(),
        }
    }
}

impl<Key, Entity: HashTableEntity<Key>> Iterator for HashTableIter<Key, Entity> {
    type Item = *mut Entity;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO:
        if let Some(entity) = self.zero_entity.take() {
            return Some(entity);
        }

        if self.idx < self.size {
            return unsafe {
                let res = Some(self.entities.offset(self.idx));
                self.idx += 1;
                res
            };
        }

        None
    }
}
