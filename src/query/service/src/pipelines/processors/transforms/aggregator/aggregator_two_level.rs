use common_datablocks::DataBlock;
use common_datablocks::HashMethod;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::FastHash;
use common_hashtable::HashtableLike;

use crate::pipelines::processors::transforms::aggregator::FinalAggregator;
use crate::pipelines::processors::transforms::aggregator::PartialAggregator;
use crate::pipelines::processors::transforms::aggregator::SingleStateAggregator;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::group_by::TwoLevelHashMethod;
use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;

pub trait TwoLevelAggregatorLike
where Self: Aggregator + Send
{
    const SUPPORT_TWO_LEVEL: bool;

    type TwoLevelAggregator: Aggregator;

    fn get_state_cardinality(&self) -> usize {
        0
    }

    fn convert_two_level(self) -> Result<TwoLevelAggregator<Self>> {
        Err(ErrorCode::Unimplemented(format!(
            "Two level aggregator is unimplemented for {}",
            Self::NAME
        )))
    }
}

impl<Method> TwoLevelAggregatorLike for PartialAggregator<true, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    type TwoLevelAggregator = PartialAggregator<true, TwoLevelHashMethod<Method>>;

    const SUPPORT_TWO_LEVEL: bool = true;

    fn get_state_cardinality(&self) -> usize {
        self.hash_table.len()
    }

    fn convert_two_level(mut self) -> Result<TwoLevelAggregator<Self>> {
        Ok(TwoLevelAggregator::<Self> {
            inner: PartialAggregator::<true, TwoLevelHashMethod<Method>> {
                ctx: self.ctx.clone(),
                area: self.area.take(),
                params: self.params.clone(),
                is_generated: self.is_generated,
                states_dropped: self.states_dropped,
                method: unimplemented!(),
                hash_table: unimplemented!(),
            },
        })
    }
}

impl<Method> TwoLevelAggregatorLike for PartialAggregator<false, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    type TwoLevelAggregator = PartialAggregator<true, TwoLevelHashMethod<Method>>;

    const SUPPORT_TWO_LEVEL: bool = true;

    fn get_state_cardinality(&self) -> usize {
        self.hash_table.len()
    }

    fn convert_two_level(mut self) -> Result<TwoLevelAggregator<Self>> {
        Ok(TwoLevelAggregator::<Self> {
            inner: PartialAggregator::<true, TwoLevelHashMethod<Method>> {
                ctx: self.ctx.clone(),
                area: self.area.take(),
                params: self.params.clone(),
                is_generated: self.is_generated,
                states_dropped: self.states_dropped,
                method: unimplemented!(),
                hash_table: unimplemented!(),
            },
        })
    }
}

impl TwoLevelAggregatorLike for SingleStateAggregator<true> {
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = SingleStateAggregator<true>;
}

impl TwoLevelAggregatorLike for SingleStateAggregator<false> {
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = SingleStateAggregator<false>;
}

impl<Method> TwoLevelAggregatorLike for FinalAggregator<true, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = FinalAggregator<true, TwoLevelHashMethod<Method>>;
}

impl<Method> TwoLevelAggregatorLike for FinalAggregator<false, Method>
where
    Method: HashMethod + PolymorphicKeysHelper<Method> + Send,
    Method::HashKey: FastHash,
{
    const SUPPORT_TWO_LEVEL: bool = false;
    type TwoLevelAggregator = FinalAggregator<true, TwoLevelHashMethod<Method>>;
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
