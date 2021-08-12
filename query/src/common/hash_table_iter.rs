use std::marker::PhantomData;
use crate::common::hash_table_entity::IHashTableEntity;

pub struct HashTableIter<Key, HashTableEntity: IHashTableEntity<Key>> {
    idx: isize,
    size: isize,
    entities: *mut HashTableEntity,
    zero_entity: Option<*mut HashTableEntity>,
    ss: PhantomData<Key>,

}

impl<Key, HashTableEntity: IHashTableEntity<Key>> HashTableIter<Key, HashTableEntity> {
    pub fn new(size: isize, entities: *mut HashTableEntity, zero_entity: Option<*mut HashTableEntity>) -> Self {
        Self {
            idx: 0,
            size,
            entities,
            zero_entity,
            ss: PhantomData::default(),
        }
    }
}

impl<Key, HashTableEntity: IHashTableEntity<Key>> Iterator for HashTableIter<Key, HashTableEntity> {
    type Item = *mut HashTableEntity;

    fn next(&mut self) -> Option<Self::Item> {
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
