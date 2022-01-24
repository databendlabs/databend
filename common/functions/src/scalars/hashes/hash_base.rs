// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::sync::Arc;

use common_datavalues2::for_all_integer_types;
use common_datavalues2::prelude::*;
use common_datavalues2::with_match_scalar_types_error;
use common_exception::Result;
use num::FromPrimitive;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;
use crate::scalars::ScalarUnaryExpression;

/// H ---> Hasher
/// R ---> Result Type
#[derive(Clone, Debug)]
pub struct BaseHashFunction<H, R> {
    display_name: String,
    h: PhantomData<H>,
    r: PhantomData<R>,
}

impl<H, R> BaseHashFunction<H, R>
where
    H: Hasher + Default + Clone + Sync + Send + 'static,
    R: Scalar + Clone + FromPrimitive + ToDataType + Sync + Send,
{
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(BaseHashFunction::<H, R> {
            display_name: display_name.to_string(),
            h: PhantomData,
            r: PhantomData,
        }))
    }

    pub fn desc() -> Function2Description {
        let mut features = FunctionFeatures::default().num_arguments(1);
        features = features.deterministic();
        Function2Description::creator(Box::new(Self::try_create)).features(features)
    }

    fn exec<S>(column: &ColumnRef) -> Result<ColumnRef>
    where
        S: Scalar,
        for<'a> <S as Scalar>::RefType<'a>: DFHash,
    {
        let unary = ScalarUnaryExpression::<S, R, _>::new(|a| {
            let mut h = H::default();
            a.hash(&mut h);
            R::from_u64(h.finish()).unwrap()
        });

        let col = unary.eval(column)?;
        Ok(Arc::new(col))
    }
}

impl<H, R> Function2 for BaseHashFunction<H, R>
where
    H: Hasher + Default + Clone + Sync + Send + 'static,
    R: Scalar + Clone + FromPrimitive + ToDataType + Sync + Send,
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(
        &self,
        _args: &[&common_datavalues2::DataTypePtr],
    ) -> Result<common_datavalues2::DataTypePtr> {
        Ok(R::to_data_type())
    }

    fn eval(
        &self,
        columns: &common_datavalues2::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues2::ColumnRef> {
        with_match_scalar_types_error!(columns[0].data_type().data_type_id().to_physical_type(), |$T| {
             Self::exec::<$T>(columns[0].column())
        })
    }
}

impl<H, R> fmt::Display for BaseHashFunction<H, R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

pub trait DFHash {
    fn hash<H: Hasher>(&self, state: &mut H);
}

macro_rules! integer_impl {
    ([], $( { $S: ident} ),*) => {
        $(
            impl DFHash for $S {
                #[inline]
                fn hash<H: Hasher>(&self, state: &mut H) {
                    Hash::hash(self, state);
                }
            }
        )*
    }
}

for_all_integer_types! { integer_impl}

impl DFHash for f32 {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        let u = self.to_bits();
        Hash::hash(&u, state);
    }
}

impl DFHash for f64 {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        let u = self.to_bits();
        Hash::hash(&u, state);
    }
}

impl<'a> DFHash for bool {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(self, state);
    }
}

impl<'a> DFHash for &'a [u8] {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash_slice(self, state);
    }
}
