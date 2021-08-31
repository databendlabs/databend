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
    pub fn create(entities: Entities<Key>, capacity: isize) -> Self {
        ShortFixedKeysStateIterator::<Key> {
            index: 0,
            capacity,
            entities,
        }
    }
}

impl<Key: ShortFixedKeyable> Iterator for ShortFixedKeysStateIterator<Key> {
    type Item = *mut ShortFixedKeysStateEntity<Key>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            while self.index < self.capacity {
                let entity = self.entities.offset(self.index);
                self.index += 1;

                if (*entity).fill {
                    return Some(entity);
                }
            }

            None
        }
    }
}
