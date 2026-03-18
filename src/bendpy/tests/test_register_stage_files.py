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
CSV_DIR = ROOT / "tests" / "data" / "csv"
TSV_DIR = ROOT / "tests" / "data" / "tsv"
CSV_PATH = ROOT / "tests" / "data" / "csv" / "select.csv"
TSV_PATH = ROOT / "tests" / "data" / "tsv" / "select.tsv"


class TestRegisterDelimitedFiles:
    def setup_method(self):
        self.ctx = SessionContext()

    def assert_view_rows(self, view_name):
        df = self.ctx.sql(f"select * from {view_name} order by column_1").to_pandas()
        assert df.values.tolist() == [[1, None, None], [2, "b", "B"], [3, "c", None]]

    def test_register_csv_select_star(self):
        self.ctx.register_csv("csv_stage_view", str(CSV_PATH))
        self.assert_view_rows("csv_stage_view")

    def test_register_csv_keeps_header_row_as_data(self, tmp_path):
        header_csv = tmp_path / "header.csv"
        header_csv.write_text("id,name\n1,alice\n2,bob\n", encoding="utf-8")

        self.ctx.register_csv("csv_header_view", str(header_csv))

        df = self.ctx.sql(
            "select column_1, column_2 from csv_header_view order by column_1"
        ).to_pandas()
        assert df.values.tolist() == [["1", "alice"], ["2", "bob"], ["id", "name"]]

    def test_register_csv_select_star_with_pattern(self):
        self.ctx.register_csv("csv_stage_pattern_view", str(CSV_DIR), pattern="select.csv")
        self.assert_view_rows("csv_stage_pattern_view")

    def test_register_tsv_select_star(self):
        self.ctx.register_tsv("tsv_stage_view", str(TSV_PATH))
        self.assert_view_rows("tsv_stage_view")

    def test_register_tsv_keeps_header_row_as_data(self, tmp_path):
        header_tsv = tmp_path / "header.tsv"
        header_tsv.write_text("id\tname\n1\talice\n2\tbob\n", encoding="utf-8")

        self.ctx.register_tsv("tsv_header_view", str(header_tsv))

        df = self.ctx.sql(
            "select column_1, column_2 from tsv_header_view order by column_1"
        ).to_pandas()
        assert df.values.tolist() == [["1", "alice"], ["2", "bob"], ["id", "name"]]

    def test_register_tsv_select_star_with_pattern(self):
        self.ctx.register_tsv("tsv_stage_pattern_view", str(TSV_DIR), pattern="select.tsv")
        self.assert_view_rows("tsv_stage_pattern_view")
