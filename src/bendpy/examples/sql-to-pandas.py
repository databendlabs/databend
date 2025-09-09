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

# Install required dependency: uv pip install matplotlib

import databend
import matplotlib.pyplot as plt

ctx = databend.SessionContext()

# Execute SQL using numbers system table
df = ctx.sql(
    "select "
    "  case "
    "    when number < 100 then 'Low'"
    "    when number < 500 then 'Medium'"
    "    else 'High'"
    "  end as category,"
    "  count(*) as count "
    "from numbers(1000) "
    "group by category "
    "order by category"
)

# Convert to Pandas
pandas_df = df.to_pandas()

# Display the DataFrame
print("Data from Databend:")
print(pandas_df)

# Create a chart
fig = pandas_df.plot(
    x="category",
    y="count", 
    kind="bar", 
    title="Number Distribution by Category"
).get_figure()
fig.savefig("numbers_chart.png")

print("\nChart saved as 'numbers_chart.png'")