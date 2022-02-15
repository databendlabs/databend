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
            type Date16Result = u16;
            type Date32Result = i32;

            fn eval_date16<R: PrimitiveType + AsPrimitive<i64>>(
                l: u16,
                r: R::RefType<'_>,
                ctx: &mut EvalContext,
            ) -> Self::Date16Result {
                define_date_add_year_months!(l, r, ctx, u16, $op)
            }

            fn eval_date32<R: PrimitiveType + AsPrimitive<i64>>(
                l: i32,
                r: R::RefType<'_>,
                ctx: &mut EvalContext,
            ) -> Self::Date32Result {
                define_date_add_year_months!(l, r, ctx, i32, $op)
            }

            fn eval_datetime32<R: PrimitiveType + AsPrimitive<i64>>(
                l: u32,
                r: R::RefType<'_>,
                ctx: &mut EvalContext,
            ) -> u32 {
                define_datetime32_add_year_months!(l, r, ctx, $op)
            }

            fn eval_datetime64<R: PrimitiveType + AsPrimitive<i64>>(
                l: i64,
                r: R::RefType<'_>,
                ctx: &mut EvalContext,
            ) -> i64 {
                define_datetime64_add_year_months!(l, r, ctx, $op)
            }
        }
    };
}

#[macro_export]
macro_rules! define_date_add_year_months {
    ($l: ident, $r: ident, $ctx: ident, $date_type:ident, $op: expr) => {{
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
        let new_date = $op(
            date.year(),
            date.month(),
            date.day(),
            $r.to_owned_scalar().as_() * factor,
        );
        new_date.map_or_else(
            |e| {
                $ctx.set_error(e);
                0
            },
            |d| d.signed_duration_since(epoch).num_days() as $date_type,
        )
    }};
}

#[macro_export]
macro_rules! define_datetime32_add_year_months {
    ($l: ident, $r: ident, $ctx: ident, $op: expr) => {{
        let factor = $ctx.factor;
        let naive = NaiveDateTime::from_timestamp_opt($l as i64, 0);
        if naive.is_none() {
            $ctx.set_error(ErrorCode::Overflow(format!(
                "Overflow on datetime with seconds {}",
                $l
            )));
            return 0;
        }

        let date = naive.unwrap();
        let new_date = $op(
            date.year(),
            date.month(),
            date.day(),
            $r.to_owned_scalar().as_() * factor,
        );
        new_date.map_or_else(
            |e| {
                $ctx.set_error(e);
                0
            },
            |d| NaiveDateTime::new(d, date.time()).timestamp() as u32,
        )
    }};
}

#[macro_export]
macro_rules! define_datetime64_add_year_months {
    ($l: ident, $r: ident, $ctx: ident, $op: expr) => {{
        let factor = $ctx.factor;
        let precision = $ctx
            .get_meta_value("precision".to_string())
            .map_or(0, |v| v.parse::<u32>().unwrap());
        let base = 10_i64.pow(9 - precision);
        let nano = $l * base;
        let naive =
            NaiveDateTime::from_timestamp_opt(nano / 1_000_000_000, (nano % 1_000_000_000) as u32);
        if naive.is_none() {
            $ctx.set_error(ErrorCode::Overflow(format!(
                "Overflow on datetime with nanoseconds {}",
                $l
            )));
            return 0;
        };

        let date = naive.unwrap();
        let new_date = $op(
            date.year(),
            date.month(),
            date.day(),
            $r.to_owned_scalar().as_() * factor,
        );
        new_date.map_or_else(
            |e| {
                $ctx.set_error(e);
                0
            },
            |d| NaiveDateTime::new(d, date.time()).timestamp_nanos() / base,
        )
    }};
}
