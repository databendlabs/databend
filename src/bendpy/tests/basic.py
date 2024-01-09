## Copyright 2021 Datafuse Labs
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.


from databend import SessionContext
import pandas as pd
import polars

class TestBasic:
    ctx = SessionContext()

    def test_simple(self):
        df =  self.ctx.sql("select number % 3 n, sum(number) b from numbers(100) group by n order by n").to_pandas()
        assert df.values.tolist() == [[0, 1683], [1, 1617], [2, 1650] ]

        df =  self.ctx.sql("select number % 3 n, sum(number) b from numbers(100) group by n order by n").collect()
        assert str(df) == """┌─────────────────────┐
│   n   │      b      │
│ UInt8 │ UInt64 NULL │
├───────┼─────────────┤
│     0 │ 1683        │
│     1 │ 1617        │
│     2 │ 1650        │
└─────────────────────┘"""

    def test_create_insert_select(self):
        self.ctx.sql("create table aa (a int, b string, c bool, d double)").collect()
        self.ctx.sql("insert into aa select number, number, true, number from numbers(10)").collect()
        self.ctx.sql("insert into aa select number, number, true, number from numbers(10)").collect()
        df = self.ctx.sql("select sum(a) x, max(b) y, max(d) z from aa where c").to_pandas()
        assert df.values.tolist() == [[90.0, '9', 9.0]]

        df = self.ctx.sql("select sum(a) x, max(b) y, max(d) z from aa where c").to_polars()
        assert df.to_pandas().values.tolist() == [[90.0, '9', 9.0]]
