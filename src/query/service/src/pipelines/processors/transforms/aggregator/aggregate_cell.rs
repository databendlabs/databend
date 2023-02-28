use std::marker::PhantomData;
use std::ops::Deref;
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
pub struct HashTableCell<T: HashtableLike> {
    inner: Option<T>,
    arena: Option<Area>,
    arena_holders: Vec<ArenaHolder>,
    temp_values: Vec<T::Value>,
    _dropper: Option<Arc<dyn HashTableDropper<T>>>,
}

unsafe impl<T: HashtableLike + Send> Send for HashTableCell<T> {}

unsafe impl<T: HashtableLike + Sync> Sync for HashTableCell<T> {}

impl<T: HashtableLike> Drop for HashTableCell<T> {
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

impl<T: HashtableLike> HashTableCell<T> {
    pub fn create(inner: T, _dropper: Arc<dyn HashTableDropper<T>>) -> HashTableCell<T> {
        HashTableCell::<T> {
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

    pub fn get_hash_table(&self) -> &T {
        match &self.inner {
            None => unreachable!(),
            Some(inner) => inner,
        }
    }

    pub fn get_hash_table_mut(&mut self) -> &mut T {
        match &mut self.inner {
            None => unreachable!(),
            Some(inner) => inner,
        }
    }

    pub fn freeze_arena(mut self) -> HashTableCell<T> {
        self.arena_holders
            .push(ArenaHolder::create(self.arena.take()));
        self
    }

    pub fn extend_holders(&mut self, holders: Vec<ArenaHolder>) {
        self.arena_holders.extend(holders)
    }

    pub fn add_temp_values(&mut self, value: T::Value) {
        self.temp_values.push(value)
    }

    pub fn into_inner(
        mut self,
    ) -> (
        T,
        Vec<ArenaHolder>,
        Vec<T::Value>,
        Arc<dyn HashTableDropper<T>>,
    ) {
        (
            self.inner.take().unwrap(),
            std::mem::take(&mut self.arena_holders),
            std::mem::take(&mut self.temp_values),
            self._dropper.take().unwrap(),
        )
    }
}

pub trait HashTableDropper<T: HashtableLike> {
    fn destroy(&self, hashtable: T);
    fn destroy_value(&self, value: &T::Value);
}

pub struct GroupByHashTableDropper<T: HashtableLike<Value = ()> + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: HashtableLike<Value = ()> + 'static> GroupByHashTableDropper<T> {
    pub fn create() -> Arc<dyn HashTableDropper<T>> {
        Arc::new(GroupByHashTableDropper::<T> {
            _phantom: Default::default(),
        })
    }
}

impl<T: HashtableLike<Value = ()> + 'static> HashTableDropper<T> for GroupByHashTableDropper<T> {
    fn destroy(&self, _: T) {
        // do nothing
    }

    fn destroy_value(&self, _: &T::Value) {
        // do nothing
    }
}

pub struct AggregateHashTableDropper<T: HashtableLike<Value = usize> + 'static> {
    params: Arc<AggregatorParams>,
    _phantom: PhantomData<T>,
}

impl<T: HashtableLike<Value = usize> + 'static> AggregateHashTableDropper<T> {
    pub fn create(params: Arc<AggregatorParams>) -> Arc<dyn HashTableDropper<T>> {
        Arc::new(AggregateHashTableDropper::<T> {
            params,
            _phantom: Default::default(),
        })
    }
}

impl<T: HashtableLike<Value = usize> + 'static> HashTableDropper<T>
    for AggregateHashTableDropper<T>
{
    fn destroy(&self, hashtable: T) {
        // TODO:
    }

    fn destroy_value(&self, value: &usize) {
        todo!()
    }
}

pub struct PartitionedHashTableDropper<Method: HashMethodBounds, V: Send + Sync + 'static> {
    _inner_dropper: Arc<dyn HashTableDropper<Method::HashTable<V>>>,
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> PartitionedHashTableDropper<Method, V> {
    pub fn create(
        _inner_dropper: Arc<dyn HashTableDropper<Method::HashTable<V>>>,
    ) -> Arc<
        dyn HashTableDropper<
            <PartitionedHashMethod<Method> as PolymorphicKeysHelper<
                PartitionedHashMethod<Method>,
            >>::HashTable<V>,
        >,
    > {
        Arc::new(Self { _inner_dropper })
    }

    pub fn get_inner(&self) -> Arc<dyn HashTableDropper<Method::HashTable<V>>> {
        self._inner_dropper.clone()
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> HashTableDropper<<PartitionedHashMethod<Method> as PolymorphicKeysHelper<PartitionedHashMethod<Method>>>::HashTable<V>>
for PartitionedHashTableDropper<Method, V>
{
    fn destroy(&self, hashtable: <PartitionedHashMethod<Method> as PolymorphicKeysHelper<PartitionedHashMethod<Method>>>::HashTable<V>) {
        for inner_table in hashtable.into_iter_tables() {
            self._inner_dropper.destroy(inner_table)
        }
    }

    fn destroy_value(&self, value: &<<PartitionedHashMethod<Method> as PolymorphicKeysHelper<PartitionedHashMethod<Method>>>::HashTable<V> as HashtableLike>::Value) {
        self._inner_dropper.destroy_value(value)
    }
}
