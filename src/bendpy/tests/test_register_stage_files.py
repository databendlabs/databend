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

from pathlib import Path

from databend import SessionContext


ROOT = Path(__file__).resolve().parents[3]
CSV_PATH = ROOT / "tests" / "data" / "csv" / "select.csv"
TSV_PATH = ROOT / "tests" / "data" / "tsv" / "select.tsv"


class TestRegisterDelimitedFiles:
    def setup_method(self):
        self.ctx = SessionContext()

    def test_register_csv_select_star(self):
        self.ctx.register_csv("csv_stage_view", str(CSV_PATH))

        df = self.ctx.sql(
            "select * from csv_stage_view order by column_1"
        ).to_pandas()
        assert df.values.tolist() == [[1, None, None], [2, "b", "B"], [3, "c", None]]

    def test_register_tsv_select_star(self):
        self.ctx.register_tsv("tsv_stage_view", str(TSV_PATH))

        df = self.ctx.sql(
            "select * from tsv_stage_view order by column_1"
        ).to_pandas()
        assert df.values.tolist() == [[1, None, None], [2, "b", "B"], [3, "c", None]]
