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

#[macro_export]
macro_rules! impl_interval_year_month {
    ($name: ident, $op: expr) => {
        #[derive(Clone)]
        pub struct $name;

        impl IntervalArithmeticImpl for $name {
            type DateResultType = i32;

            fn eval_date(
                l: i32,
                r: impl AsPrimitive<i64>,
                ctx: &mut EvalContext,
            ) -> Self::DateResultType {
                define_date_add_year_months!(l, r, ctx, $op)
            }

            fn eval_timestamp(l: i64, r: impl AsPrimitive<i64>, ctx: &mut EvalContext) -> i64 {
                define_timestamp_add_year_months!(l, r, ctx, $op)
            }
        }
    };
}

#[macro_export]
macro_rules! define_date_add_year_months {
    ($l: ident, $r: ident, $ctx: ident, $op: expr) => {{
        let factor = $ctx.factor;
        let epoch = NaiveDate::from_ymd(1970, 1, 1);
        let naive = epoch.checked_add_signed(Duration::days($l as i64));
        if naive.is_none() {
            $ctx.set_error(ErrorCode::Overflow(format!(
                "Overflow on date with days {}.",
                $l
            )));
            return 0;
        }

        let date = naive.unwrap();
        let new_date = $op(date.year(), date.month(), date.day(), $r.as_() * factor);
        new_date.map_or_else(
            |e| {
                $ctx.set_error(e);
                0
            },
            |d| d.signed_duration_since(epoch).num_days() as i32,
        )
    }};
}

#[macro_export]
macro_rules! define_timestamp_add_year_months {
    ($l: ident, $r: ident, $ctx: ident, $op: expr) => {{
        let factor = $ctx.factor;
        let precision = $ctx.precision as u32;
        let base = 10_i64.pow(6 - precision);
        let micros = $l * base;
        let naive = NaiveDateTime::from_timestamp_opt(
            micros / 1_000_000,
            (micros % 1_000_000 * 1000) as u32,
        );
        if naive.is_none() {
            $ctx.set_error(ErrorCode::Overflow(format!(
                "Overflow on datetime with microseconds {}",
                $l
            )));
            return 0;
        };

        let date = naive.unwrap();
        let new_date = $op(date.year(), date.month(), date.day(), $r.as_() * factor);
        new_date.map_or_else(
            |e| {
                $ctx.set_error(e);
                0
            },
            |d| NaiveDateTime::new(d, date.time()).timestamp_micros() / base,
        )
    }};
}
