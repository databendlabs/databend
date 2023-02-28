use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use common_hashtable::FastHash;
use common_hashtable::HashtableLike;
use common_hashtable::PartitionedHashMap;
use common_jsonb::Value;

use crate::pipelines::processors::transforms::group_by::Area;
use crate::pipelines::processors::transforms::group_by::ArenaHolder;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::AggregatorParams;

//
pub struct HashTableCell<T: HashMethodBounds, V: Send + Sync + 'static> {
    inner: Option<T::HashTable<V>>,
    arena: Option<Area>,
    arena_holders: Vec<ArenaHolder>,
    temp_values: Vec<<T::HashTable<V> as HashtableLike>::Value>,
    _dropper: Option<Arc<dyn HashTableDropper<T, V>>>,
}

unsafe impl<T: HashMethodBounds, V: Send + Sync + 'static> Send for HashTableCell<T, V> {}

unsafe impl<T: HashMethodBounds, V: Send + Sync + 'static> Sync for HashTableCell<T, V> {}

impl<T: HashMethodBounds, V: Send + Sync + 'static> Deref for HashTableCell<T, V> {
    type Target = T::HashTable<V>;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}

impl<T: HashMethodBounds, V: Send + Sync + 'static> DerefMut for HashTableCell<T, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap()
    }
}

impl<T: HashMethodBounds, V: Send + Sync + 'static> Drop for HashTableCell<T, V> {
    fn drop(&mut self) {
        if let Some(dropper) = self._dropper.take() {
            if Arc::strong_count(&dropper) == 1 {
                dropper.destroy(self.inner.take().unwrap());

                for value in &std::mem::take(&mut self.temp_values) {
                    dropper.destroy_value(value)
                }
            }
        }
    }
}

impl<T: HashMethodBounds, V: Send + Sync + 'static> HashTableCell<T, V> {
    pub fn create(
        inner: T::HashTable<V>,
        _dropper: Arc<dyn HashTableDropper<T, V>>,
    ) -> HashTableCell<T, V> {
        HashTableCell::<T, V> {
            inner: Some(inner),
            arena_holders: vec![],
            temp_values: vec![],
            _dropper: Some(_dropper),
            arena: Some(Area::create()),
        }
    }

    pub fn get_arena_mut(&mut self) -> Option<&mut Area> {
        self.arena.as_mut()
    }

    pub fn freeze_arena(mut self) -> HashTableCell<T, V> {
        self.arena_holders.push(ArenaHolder::create(self.arena.take()));
        self
    }

    pub fn extend_holders(&mut self, holders: Vec<ArenaHolder>) {
        self.arena_holders.extend(holders)
    }

    pub fn add_temp_values(&mut self, value: <T::HashTable<V> as HashtableLike>::Value) {
        self.temp_values.push(value)
    }

    pub fn into_inner(
        mut self,
    ) -> (
        T::HashTable<V>,
        Vec<ArenaHolder>,
        Vec<<T::HashTable<V> as HashtableLike>::Value>,
        Arc<dyn HashTableDropper<T, V>>,
    ) {
        (
            self.inner.take().unwrap(),
            std::mem::take(&mut self.arena_holders),
            std::mem::take(&mut self.temp_values),
            self._dropper.take().unwrap(),
        )
    }
}

pub trait HashTableDropper<T: HashMethodBounds, V: Send + Sync + 'static> {
    fn destroy(&self, hashtable: T::HashTable<V>);
    fn destroy_value(&self, value: &<T::HashTable<V> as HashtableLike>::Value);
}

pub struct GroupByHashTableDropper<T: HashMethodBounds> {
    _phantom: PhantomData<T>,
}

impl<T: HashMethodBounds> GroupByHashTableDropper<T> {
    pub fn create() -> Arc<dyn HashTableDropper<T, ()>> {
        Arc::new(GroupByHashTableDropper::<T> {
            _phantom: Default::default(),
        })
    }
}

impl<T: HashMethodBounds> HashTableDropper<T, ()> for GroupByHashTableDropper<T> {
    fn destroy(&self, _: T::HashTable<()>) {
        // do nothing
    }

    fn destroy_value(&self, _: &()) {
        // do nothing
    }
}

pub struct AggregateHashTableDropper<T: HashMethodBounds> {
    params: Arc<AggregatorParams>,
    _phantom: PhantomData<T>,
}

impl<T: HashMethodBounds> AggregateHashTableDropper<T> {
    pub fn create(params: Arc<AggregatorParams>) -> Arc<dyn HashTableDropper<T, usize>> {
        Arc::new(AggregateHashTableDropper::<T> {
            params,
            _phantom: Default::default(),
        })
    }
}

impl<T: HashMethodBounds> HashTableDropper<T, usize> for AggregateHashTableDropper<T> {
    fn destroy(&self, hashtable: T::HashTable<usize>) {
        // TODO:
    }

    fn destroy_value(&self, value: &usize) {
        todo!()
    }
}

pub struct PartitionedHashTableDropper<Method: HashMethodBounds, V: Send + Sync + 'static> {
    _inner_dropper: Arc<dyn HashTableDropper<Method, V>>,
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> PartitionedHashTableDropper<Method, V> {
    pub fn create(
        _inner_dropper: Arc<dyn HashTableDropper<Method, V>>,
    ) -> Arc<dyn HashTableDropper<PartitionedHashMethod<Method>, V>> {
        Arc::new(Self { _inner_dropper })
    }

    pub fn get_inner(&self) -> Arc<dyn HashTableDropper<Method, V>> {
        self._inner_dropper.clone()
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static>
HashTableDropper<PartitionedHashMethod<Method>, V> for PartitionedHashTableDropper<Method, V>
{
    fn destroy(
        &self,
        hashtable: <PartitionedHashMethod<Method> as PolymorphicKeysHelper<
            PartitionedHashMethod<Method>,
        >>::HashTable<V>,
    ) {
        for inner_table in hashtable.into_iter_tables() {
            self._inner_dropper.destroy(inner_table)
        }
    }

    fn destroy_value(
        &self,
        value: &<<PartitionedHashMethod<Method> as PolymorphicKeysHelper<
            PartitionedHashMethod<Method>,
        >>::HashTable<V> as HashtableLike>::Value,
    ) {
        self._inner_dropper.destroy_value(value)
    }
}
