use common_ast::parser::statement::create_table_source;
use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_exception::{ErrorCode, Result};
use common_hashtable::{HashtableLike, TwoLevelHashMap};

use crate::pipelines::processors::transforms::aggregator::PartialAggregator;
use crate::pipelines::processors::transforms::group_by::{PolymorphicKeysHelper, TwoLevelHashMethod};
use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;

pub trait TwoLevelAggregatorLike where Self: Aggregator + Send {
    type TwoLevelAggregator: Aggregator;

    fn get_state_cardinality(&self) -> usize;

    fn convert_two_level(self) -> Result<TwoLevelAggregator<Self>> {
        Err(ErrorCode::Unimplemented(format!(
            "Two level aggregator is unimplemented for {}",
            Self::NAME
        )))
    }
}

impl<const HAS_AGG: bool, Method> TwoLevelAggregatorLike for PartialAggregator<HAS_AGG, Method>
    where Method: HashMethod + PolymorphicKeysHelper<Method> + Send {
    type TwoLevelAggregator = PartialAggregator<HAS_AGG, TwoLevelHashMethod<Method>>;

    fn get_state_cardinality(&self) -> usize {
        self.hash_table.len()
    }

    fn convert_two_level(self) -> Result<TwoLevelAggregator<Self>> {
        Ok(TwoLevelAggregator::<Self> {
            inner: PartialAggregator::<HAS_AGG, TwoLevelHashMethod<Method>> {
                ctx: self.ctx,
                area: self.area,
                params: self.params,
                is_generated: self.is_generated,
                states_dropped: self.states_dropped,
                method: unimplemented!(),
                hash_table: unimplemented!(),
            }
        })
    }
}


pub struct TwoLevelAggregator<T: TwoLevelAggregatorLike> {
    inner: T::TwoLevelAggregator,
}

impl<T: TwoLevelAggregatorLike> Aggregator for TwoLevelAggregator<T> {
    const NAME: &'static str = "TwoLevelAggregator";

    #[inline(always)]
    fn consume(&mut self, data: DataBlock) -> Result<()> {
        self.inner.consume(data)
    }

    #[inline(always)]
    fn generate(&mut self) -> Result<Option<DataBlock>> {
        self.inner.generate()
    }
}
