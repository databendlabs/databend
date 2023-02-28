use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

use common_hashtable::HashtableLike;

use crate::pipelines::processors::transforms::group_by::Area;
use crate::pipelines::processors::transforms::group_by::ArenaHolder;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::AggregatorParams;

//
pub struct HashTableCell<T: HashMethodBounds, V: Send + Sync + 'static> {
    pub hashtable: T::HashTable<V>,
    pub arena: Area,
    pub arena_holders: Vec<ArenaHolder>,
    pub temp_values: Vec<<T::HashTable<V> as HashtableLike>::Value>,
    pub _dropper: Option<Arc<dyn HashTableDropper<T, V>>>,
}

unsafe impl<T: HashMethodBounds, V: Send + Sync + 'static> Send for HashTableCell<T, V> {}

unsafe impl<T: HashMethodBounds, V: Send + Sync + 'static> Sync for HashTableCell<T, V> {}

impl<T: HashMethodBounds, V: Send + Sync + 'static> Drop for HashTableCell<T, V> {
    fn drop(&mut self) {
        if let Some(dropper) = self._dropper.take() {
            dropper.destroy(&mut self.hashtable);

            for value in &std::mem::take(&mut self.temp_values) {
                dropper.destroy_value(value)
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
            hashtable: inner,
            arena_holders: vec![],
            temp_values: vec![],
            _dropper: Some(_dropper),
            arena: Area::create(),
        }
    }

    pub fn extend_holders(&mut self, holders: Vec<ArenaHolder>) {
        self.arena_holders.extend(holders)
    }

    pub fn extend_temp_values(&mut self, values: Vec<<T::HashTable<V> as HashtableLike>::Value>) {
        self.temp_values.extend(values)
    }
}

pub trait HashTableDropper<T: HashMethodBounds, V: Send + Sync + 'static> {
    fn as_any(&self) -> &dyn Any;
    fn destroy(&self, hashtable: &mut T::HashTable<V>);
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn destroy(&self, _: &mut T::HashTable<()>) {
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn destroy(&self, hashtable: &mut T::HashTable<usize>) {
        // TODO:
    }

    fn destroy_value(&self, value: &usize) {
        // todo!()
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

    pub fn split_cell(
        mut v: HashTableCell<PartitionedHashMethod<Method>, V>,
    ) -> Vec<HashTableCell<Method, V>> {
        unsafe {
            let dropper = v
                ._dropper
                .as_ref()
                .unwrap()
                .as_any()
                .downcast_ref::<PartitionedHashTableDropper<Method, V>>()
                .unwrap()
                ._inner_dropper
                .clone();

            let mut cells = Vec::with_capacity(256);
            while let Some(table) = v.hashtable.pop_first_inner_table() {
                cells.push(HashTableCell::create(table, dropper.clone()));
            }

            cells
        }
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static>
    HashTableDropper<PartitionedHashMethod<Method>, V> for PartitionedHashTableDropper<Method, V>
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn destroy(
        &self,
        hashtable: &mut <PartitionedHashMethod<Method> as PolymorphicKeysHelper<
            PartitionedHashMethod<Method>,
        >>::HashTable<V>,
    ) {
        for inner_table in hashtable.iter_tables_mut() {
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
