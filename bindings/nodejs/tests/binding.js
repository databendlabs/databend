/*
 * Copyright 2021 Datafuse Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const assert = require("assert");
const { Client } = require("../index.js");
const { Given, When, Then } = require("@cucumber/cucumber");

const dsn = process.env.TEST_DATABEND_DSN
  ? process.env.TEST_DATABEND_DSN
  : "databend://root:@localhost:8000/default?sslmode=disable";

Given("A new Databend Driver Client", function () {
  this.client = new Client(dsn);
});

Then("Select String {string} should be equal to {string}", async function (input, output) {
  const row = await this.client.queryRow(`SELECT '${input}'`);
  const value = row.values()[0].toString();
  assert(value === output);
});
