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

# Create S3 connection
ctx.create_s3_connection(
    name="s3_conn",
    access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

# Create table on S3 
ctx.sql("create or replace table products (id int, name string, price float, category string) 's3://bohu/pybend/tables/' CONNECTION = (connection_name = 's3_conn')").collect()

# Insert sample data
ctx.sql("insert into products values (1, 'Laptop', 1299.99, 'Electronics'), (2, 'Coffee Mug', 12.50, 'Kitchen'), (3, 'Notebook', 5.99, 'Office')").collect()

# Show all data
ctx.sql("select * from products").show()