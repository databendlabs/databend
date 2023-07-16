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

from behave import given, when, then
from behave.api.async_step import async_run_until_complete
import databend_driver


@given("A new Databend-Driver Async Connector")
@async_run_until_complete
async def _(context):
    dsn = os.getenv(
        "TEST_DATABEND_DSN", "databend+http://root:root@localhost:8000/?sslmode=disable"
    )
    context.ad = databend_driver.AsyncDatabendDriver(dsn)


@when('Async exec "{sql}"')
@async_run_until_complete
async def _(context, sql):
    await context.ad.exec(sql)


@then('The select "{select_sql}" should run')
@async_run_until_complete
async def _(context, select_sql):
    await context.ad.exec(select_sql)
