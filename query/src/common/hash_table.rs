use std::alloc::Layout;
use std::marker::PhantomData;
use std::mem;
use crate::common::hash_table_entity::IHashTableEntity;
use crate::common::hash_table_hasher::IHasher;
use crate::common::hash_table_grower::IHashTableGrower;
use crate::common::hash_table_iter::HashTableIter;

pub struct HashTable<Key, HashTableEntity: IHashTableEntity<Key> + Sized + PartialEq, Hasher: IHasher<Key>, Grower: IHashTableGrower + Default + Clone> {
    size: usize,
    grower: Grower,
    entities: *mut HashTableEntity,
    entities_raw: *mut u8,
    zero_entity: Option<*mut HashTableEntity>,
    zero_entity_raw: Option<*mut u8>,

    /// Generics hold
    generics_hold: PhantomData<(Key, Hasher)>,
}

impl<Key, HashTableEntity: IHashTableEntity<Key> + Sized + PartialEq, Hasher: IHasher<Key>, Grower: IHashTableGrower + Default + Clone> Drop for HashTable<Key, HashTableEntity, Hasher, Grower> {
    fn drop(&mut self) {
        unsafe {
            let size = (self.grower.max_size() as usize) * mem::size_of::<HashTableEntity>();
            let layout = Layout::from_size_align_unchecked(size, std::mem::align_of::<HashTableEntity>());
            std::alloc::dealloc(self.entities_raw, layout);

            if let Some(zero_entity) = self.zero_entity_raw {
                let zero_layout = Layout::from_size_align_unchecked(mem::size_of::<HashTableEntity>(), std::mem::align_of::<HashTableEntity>());
                std::alloc::dealloc(zero_entity, zero_layout);
            }
        }
    }
}

impl<Key, HashTableEntity: IHashTableEntity<Key> + Sized + PartialEq, Hasher: IHasher<Key>, Grower: IHashTableGrower + Default + Clone> HashTable<Key, HashTableEntity, Hasher, Grower> {
    pub fn new() -> HashTable<Key, HashTableEntity, Hasher, Grower> {
        // TODO:
        let size = (1 << 8) * mem::size_of::<HashTableEntity>();
        unsafe {
            let layout = Layout::from_size_align_unchecked(size, mem::align_of::<HashTableEntity>());
            let raw_ptr = std::alloc::alloc_zeroed(layout);
            let entities_ptr = raw_ptr as *mut HashTableEntity;
            HashTable {
                size: 0,
                grower: Default::default(),
                entities: entities_ptr,
                entities_raw: raw_ptr,
                zero_entity: None,
                zero_entity_raw: None,
                generics_hold: PhantomData::default(),
            }
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline(always)]
    pub fn iter(&self) -> impl Iterator<Item=*mut HashTableEntity> {
        HashTableIter::new(self.size as isize, self.entities.clone(), self.zero_entity.clone())
    }

    #[inline(always)]
    pub fn insert_key(&mut self, key: Key, inserted: bool) -> *mut HashTableEntity {
        let hash = Hasher::hash(&key);
        match self.insert_if_zero_key(&key, hash, inserted) {
            None => self.insert_non_zero_key(&key, hash, inserted),
            Some(zero_hash_table_entity) => zero_hash_table_entity
        }
    }

    #[inline(always)]
    pub fn find_key(&self, key: &Key) -> Option<*mut HashTableEntity> {
        if !HashTableEntity::is_zero_s(key) {
            let hash_value = Hasher::hash(key);
            let place_value = self.find_entity(key, hash_value);
            unsafe {
                let value = self.entities.offset(place_value);
                return match value.as_ref().unwrap().is_zero() {
                    true => None,
                    false => Some(value)
                };
            }
        }

        self.zero_entity
    }

    #[inline(always)]
    fn find_entity(&self, key: &Key, hash_value: u64) -> isize {
        let mut place_value = self.grower.place(hash_value);
        loop {
            let entity = unsafe { self.entities.offset(place_value as isize).as_ref() }.unwrap();
            if entity.is_zero() || entity.key_equals(key, hash_value) {
                return place_value;
            }
            place_value = self.grower.next_place(place_value);
        }
    }

    #[inline(always)]
    fn insert_non_zero_key(&mut self, key: &Key, hash_value: u64, inserted: bool) -> *mut HashTableEntity {
        let place_value = self.find_entity(key, hash_value);
        self.insert_non_zero_key_impl(place_value, key, hash_value, inserted)
    }

    #[inline(always)]
    fn insert_non_zero_key_impl(&mut self, place_value: isize, key: &Key, hash_value: u64, _inserted: bool) -> *mut HashTableEntity {
        unsafe {
            let entity = self.entities.offset(place_value).as_mut().unwrap();

            if !entity.is_zero() {
                // inserted = false;
                return self.entities.offset(place_value);
            }

            self.size += 1;
            entity.set_key_and_hash(key, hash_value);

            if self.grower.overflow(self.size) {
                self.resize();
                let new_place = self.find_entity(key, hash_value);
                std::assert!(!self.entities.offset(new_place).as_ref().unwrap().is_zero());
                return self.entities.offset(place_value);
            }

            self.entities.offset(place_value)
        }
    }

    #[inline(always)]
    fn insert_if_zero_key(&mut self, key: &Key, hash_value: u64, _inserted: bool) -> Option<*mut HashTableEntity> {
        if HashTableEntity::is_zero_s(key) {
            if self.zero_entity.is_none() {
                unsafe {
                    let layout = Layout::from_size_align_unchecked(mem::size_of::<HashTableEntity>(), mem::align_of::<HashTableEntity>());

                    self.size += 1;
                    self.zero_entity_raw = Some(std::alloc::alloc_zeroed(layout));
                    self.zero_entity = Some(self.zero_entity_raw.unwrap() as *mut HashTableEntity);
                    self.zero_entity.unwrap().as_mut().unwrap().set_key_and_hash(key, hash_value);
                }
            };

            return self.zero_entity;
        }

        Option::None
    }

    unsafe fn resize(&mut self)
    {
        let old_size = self.grower.max_size();
        let mut new_grower = self.grower.clone();

        new_grower.increase_size();

        // Realloc memory
        if new_grower.max_size() > self.grower.max_size() {
            let new_size = (new_grower.max_size() as usize) * std::mem::size_of::<HashTableEntity>();
            let layout = Layout::from_size_align_unchecked(new_size, std::mem::align_of::<HashTableEntity>());
            self.entities_raw = std::alloc::realloc(self.entities_raw, layout, new_size);
            self.entities = self.entities_raw as *mut HashTableEntity;
            self.entities.offset(self.grower.max_size()).write_bytes(0, (new_grower.max_size() - self.grower.max_size()) as usize);
            self.grower = new_grower;

            for index in 0..old_size {
                let entity_ptr = self.entities.offset(index);
                let entity = entity_ptr.as_mut().unwrap();

                if !entity_ptr.as_mut().unwrap().is_zero() {
                    self.reinsert(entity_ptr, entity.get_hash());
                }
            }

            // There is also a special case:
            //      if the element was to be at the end of the old buffer,                  [        x]
            //      but is at the beginning because of the collision resolution chain,      [o       x]
            //      then after resizing, it will first be out of place again,               [        xo        ]
            //      and in order to transfer it where necessary,
            //      after transferring all the elements from the old halves you need to     [         o   x    ]
            //      process tail from the collision resolution chain immediately after it   [        o    x    ]
            for index in old_size..self.grower.max_size() {
                let entity = self.entities.offset(index).as_mut().unwrap();

                if !entity.is_zero() {
                    return;
                }
                self.reinsert(self.entities.offset(index), entity.get_hash());
            }
        }
    }

    #[inline(always)]
    unsafe fn reinsert(&self, entity: *mut HashTableEntity, hash_value: u64) -> isize {
        let mut place_value = self.grower.place(hash_value);

        if entity.as_ref().unwrap() != self.entities.offset(place_value).as_ref().unwrap() {
            place_value = self.find_entity(entity.as_ref().unwrap().get_key(), hash_value);
            if self.entities.offset(place_value).as_ref().unwrap().is_zero() {
                std::mem::swap(entity.as_mut().unwrap(), self.entities.offset(place_value).as_mut().unwrap());
            }
        }

        place_value
    }
}

// #[test]
// fn test_hash_table() {
//     let inserted = true;
//     let numbers: Vec<i32> = (0..1024).map(|_| rand::random::<i32>()).collect();
//     let mut hash_table = HashTable::<i32, DefaultHashTableEntity, DefaultHasher<i32>, DefaultHashTableGrower>::new();
//     for number in numbers.clone() {
//         hash_table.insert_key(number, inserted);
//     }
//
//     unsafe {
//         for number in numbers.clone() {
//             assert_eq!(hash_table.find_key(&number).unwrap().as_ref().unwrap().key, number);
//         }
//     }
// }
