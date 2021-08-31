use std::intrinsics::unlikely;

use crate::pipelines::transforms::group_by::aggregator_state_entity::ShortFixedKeyable;
use crate::pipelines::transforms::group_by::aggregator_state_entity::ShortFixedKeysStateEntity;
use crate::pipelines::transforms::group_by::aggregator_state_entity::StateEntity;

type Entities<Key> = *mut ShortFixedKeysStateEntity<Key>;

pub struct ShortFixedKeysStateIterator<Key: ShortFixedKeyable> {
    index: isize,
    capacity: isize,
    entities: *mut ShortFixedKeysStateEntity<Key>,
}

impl<Key: ShortFixedKeyable> ShortFixedKeysStateIterator<Key> {
    pub fn create(entities: Entities<Key>, capacity: isize, has_zero: bool) -> Self {
        match has_zero {
            true => ShortFixedKeysStateIterator::<Key> {
                index: 0,
                capacity,
                entities,
            },
            false => ShortFixedKeysStateIterator::<Key> {
                index: 1,
                capacity,
                entities,
            },
        }
    }
}

impl<Key: ShortFixedKeyable> Iterator for ShortFixedKeysStateIterator<Key> {
    type Item = *mut ShortFixedKeysStateEntity<Key>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            if unlikely(self.index == 0) {
                self.index += 1;
                return Some(self.entities.offset(self.index));
            }

            while self.index < self.capacity {
                let entity = self.entities.offset(self.index);
                self.index += 1;

                if !entity.get_state_key().is_zero_key() {
                    return Some(entity);
                }
            }

            match self.index == self.capacity {
                true => None,
                false => Some(self.entities.offset(self.index)),
            }
        }
    }
}
