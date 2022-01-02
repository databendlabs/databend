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

use std::cmp::Ordering;
use std::fmt;

static FACTORS10: &[i64] = &[
    1,
    10,
    100,
    1000,
    10000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
];

pub trait Base {
    fn scale(self, scale: i64) -> i64;
}

pub trait InternalResult {
    fn get(underlying: i64) -> Self;
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum NoBits {
    N32,
    N64,
}

/// Provides arbitrary-precision floating point decimal.
#[derive(Clone)]
pub struct Decimal {
    pub(crate) underlying: i64,
    pub(crate) nobits: NoBits, // its domain is {32, 64}
    pub(crate) precision: u8,
    pub(crate) scale: u8,
}

impl Default for Decimal {
    fn default() -> Self {
        Decimal {
            underlying: 0,
            precision: 9,
            scale: 4,
            nobits: NoBits::N32,
        }
    }
}

macro_rules! base_for {
    ( $( $t:ty: $cast:expr ),* ) => {
        $(
            impl Base for $t {
                fn scale(self, scale: i64) -> i64 {
                    $cast(self * (scale as $t)) as i64
                }
            }
        )*
    };
}

base_for! {
    f32: std::convert::identity,
    f64: std::convert::identity,
    i8: i64::from,
    i16: i64::from,
    i32: i64::from,
    i64: std::convert::identity,
    u8: i64::from,
    u16: i64::from,
    u32: i64::from,
    u64 : std::convert::identity
}

impl InternalResult for i32 {
    #[inline(always)]
    fn get(underlying: i64) -> Self {
        underlying as Self
    }
}

impl InternalResult for i64 {
    #[inline(always)]
    fn get(underlying: i64) -> Self {
        underlying
    }
}

impl NoBits {
    pub(crate) fn from_precision(precision: u8) -> Option<NoBits> {
        if precision <= 9 {
            Some(NoBits::N32)
        } else if precision <= 18 {
            Some(NoBits::N64)
        } else {
            None
        }
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        match self.scale.cmp(&other.scale) {
            Ordering::Less => {
                let delta = other.scale() - self.scale();
                let underlying = self.underlying * FACTORS10[delta];
                other.underlying == underlying
            }
            Ordering::Equal => self.underlying == other.underlying,
            Ordering::Greater => {
                let delta = self.scale() - other.scale();
                let underlying = other.underlying * FACTORS10[delta];
                self.underlying == underlying
            }
        }
    }
}

pub fn decimal2str(decimal: &Decimal) -> String {
    let mut r = format!("{}", decimal.underlying);
    while r.len() < decimal.scale() {
        r.insert(0, '0');
    }
    let pos = r.len() - decimal.scale();
    r.insert(pos, '.');
    if r.starts_with('.') {
        r.insert(0, '0');
    }
    r
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", decimal2str(self))
    }
}

impl fmt::Debug for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", decimal2str(self))
    }
}

impl From<Decimal> for f32 {
    fn from(value: Decimal) -> Self {
        value.underlying as f32 / FACTORS10[value.scale()] as f32
    }
}

impl From<Decimal> for f64 {
    fn from(value: Decimal) -> Self {
        value.underlying as f64 / FACTORS10[value.scale()] as f64
    }
}

impl Decimal {
    /// Method of creating a Decimal.
    pub fn new(underlying: i64, scale: u8) -> Decimal {
        let precision = 18;
        if scale > precision {
            panic!("scale can't be greater than 18");
        }

        Decimal {
            underlying,
            precision,
            scale,
            nobits: NoBits::N64,
        }
    }

    pub fn of<B: Base>(source: B, scale: u8) -> Decimal {
        let precision = 18;
        if scale > precision {
            panic!("scale can't be greater than 18");
        }

        let underlying = source.scale(FACTORS10[scale as usize]);
        if underlying > FACTORS10[precision as usize] as i64 {
            panic!("{} > {}", underlying, FACTORS10[precision as usize]);
        }

        Decimal {
            underlying,
            precision,
            scale,
            nobits: NoBits::N64,
        }
    }

    /*
    /// Get the internal representation of decimal as [`i32`] or [`i64`].
    ///
    /// example:
    /// ```rust
    /// # use std::env;
    /// # use clickhouse_rs::{Pool, types::Decimal, errors::Result};
    /// # let mut rt = tokio::runtime::Runtime::new().unwrap();
    /// # let ret: Result<()> = rt.block_on(async {
    /// #     let database_url = env::var("DATABASE_URL")
    /// #         .unwrap_or("tcp://localhost:9000?compression=lz4".into());
    /// #     let pool = Pool::new(database_url);
    ///       let mut c = pool.get_handle().await?;
    ///       let block = c.query("SELECT toDecimal32(2, 4) AS x").fetch_all().await?;
    ///
    ///       let x: Decimal = block.get(0, "x")?;
    ///       let actual: i32 = x.internal();
    ///       assert_eq!(20000, actual);
    /// #     Ok(())
    /// # });
    /// # ret.unwrap()
    /// ```
     */
    #[inline(always)]
    pub fn internal<I: InternalResult>(&self) -> I {
        InternalResult::get(self.underlying)
    }

    /// Determines how many decimal digits fraction can have.
    pub fn scale(&self) -> usize {
        self.scale as usize
    }

    #[must_use]
    pub fn set_scale(self, scale: u8) -> Self {
        let underlying = match scale.cmp(&self.scale) {
            Ordering::Less => {
                let delta = self.scale() - scale as usize;
                self.underlying / FACTORS10[delta]
            }
            Ordering::Equal => return self,
            Ordering::Greater => {
                let delta = scale as usize - self.scale();
                self.underlying * FACTORS10[delta]
            }
        };

        Decimal {
            underlying,
            precision: self.precision,
            scale,
            nobits: self.nobits,
        }
    }
}
