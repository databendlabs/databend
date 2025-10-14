# Copyright 2021 Datafuse Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import databend

ctx = databend.SessionContext()

# Create S3 connection and stage
ctx.create_s3_connection(
    name="s3_conn",
    access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

ctx.create_stage(
    name="my_stage", url="s3://bohu/pybend/stage/", connection_name="s3_conn"
)

# Unload generated data to stage
ctx.sql(
    "copy into @my_stage from (select number from numbers(10)) file_format = (type = parquet)"
).collect()

# List files in stage
ctx.list_stages("my_stage").show()

# Query data from stage
ctx.sql("select * from @my_stage (file_format => 'parquet')").show()

# Count records from stage
ctx.sql(
    "select count(*) as total_records from @my_stage (file_format => 'parquet')"
).show()
