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

Given("A new Databend Driver Client", async function () {
  this.client = new Client(dsn);
  this.conn = await this.client.getConn();
});

Then("Select string {string} should be equal to {string}", async function (input, output) {
  const row = await this.conn.queryRow(`SELECT '${input}'`);
  const value = row.values()[0];
  assert.equal(output, value);
});

Then("Select types should be expected native types", async function () {
  // NumberValue::Decimal
  const row1 = await this.conn.queryRow(`SELECT 15.7563::Decimal(8,4), 2.0+3.0`);
  assert.deepEqual(row1.values(), ["15.7563", "5.0"]);

  // Array
  const row2 = await this.conn.queryRow(`SELECT [10::Decimal(15,2), 1.1+2.3]`);
  assert.deepEqual(row2.values(), [["10.00", "3.40"]]);

  // Map
  const row3 = await this.conn.queryRow(`SELECT {'xx':to_date('2020-01-01')}`);
  assert.deepEqual(row3.values(), [{ xx: new Date("2020-01-01") }]);

  // Tuple
  const row4 = await this.conn.queryRow(`SELECT (10, '20', to_datetime('2024-04-16 12:34:56.789'))`);
  assert.deepEqual(row4.values(), [[10, "20", new Date("2024-04-16T12:34:56.789Z")]]);
});

Then("Select numbers should iterate all rows", async function () {
  let rows = await this.conn.queryIter("SELECT number FROM numbers(5)");
  let ret = [];
  let row = await rows.next();
  while (row) {
    ret.push(row.values()[0]);
    row = await rows.next();
  }
  const expected = [0, 1, 2, 3, 4];
  assert.deepEqual(ret, expected);
});

When("Create a test table", async function () {
  await this.conn.exec("DROP TABLE IF EXISTS test");
  await this.conn.exec(`CREATE TABLE test (
		i64 Int64,
		u64 UInt64,
		f64 Float64,
		s   String,
		s2  String,
		d   Date,
		t   DateTime
    );`);
});

Then("Insert and Select should be equal", async function () {
  await this.conn.exec(`INSERT INTO test VALUES
    (-1, 1, 1.0, '1', '1', '2011-03-06', '2011-03-06 06:20:00'),
    (-2, 2, 2.0, '2', '2', '2012-05-31', '2012-05-31 11:20:00'),
    (-3, 3, 3.0, '3', '2', '2016-04-04', '2016-04-04 11:30:00')`);
  const rows = await this.conn.queryIter("SELECT * FROM test");
  const ret = [];
  let row = await rows.next();
  while (row) {
    ret.push(row.values());
    row = await rows.next();
  }
  const expected = [
    [-1, 1, 1.0, "1", "1", new Date("2011-03-06"), new Date("2011-03-06T06:20:00Z")],
    [-2, 2, 2.0, "2", "2", new Date("2012-05-31"), new Date("2012-05-31T11:20:00Z")],
    [-3, 3, 3.0, "3", "2", new Date("2016-04-04"), new Date("2016-04-04T11:30:00Z")],
  ];
  assert.deepEqual(ret, expected);
});

Then("Stream load and Select should be equal", async function () {
  const values = [
    ["-1", "1", "1.0", "1", "1", "2011-03-06", "2011-03-06T06:20:00Z"],
    ["-2", "2", "2.0", "2", "2", "2012-05-31", "2012-05-31T11:20:00Z"],
    ["-3", "3", "3.0", "3", "2", "2016-04-04", "2016-04-04T11:30:00Z"],
  ];
  const progress = await this.conn.streamLoad(`INSERT INTO test VALUES`, values);
  assert.equal(progress.writeRows, 3);
  assert.equal(progress.writeBytes, 185);

  const rows = await this.conn.queryIter("SELECT * FROM test");
  const ret = [];
  let row = await rows.next();
  while (row) {
    ret.push(row.values());
    row = await rows.next();
  }
  const expected = [
    [-1, 1, 1.0, "1", "1", new Date("2011-03-06"), new Date("2011-03-06T06:20:00Z")],
    [-2, 2, 2.0, "2", "2", new Date("2012-05-31"), new Date("2012-05-31T11:20:00Z")],
    [-3, 3, 3.0, "3", "2", new Date("2016-04-04"), new Date("2016-04-04T11:30:00Z")],
  ];
  assert.deepEqual(ret, expected);
});
