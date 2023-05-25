# Copyright 2023 Datafuse Labs.
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


Feature: Databend-Driver Binding

    Scenario: Databend-Driver Async Operations
        Given A new Databend-Driver Async Connector
        When Async exec "CREATE TABLE if not exists test_data (x Int32,y VARCHAR)"
        When Async exec "INSERT INTO test_data(x,y) VALUES(1,'xx')"
        Then The select "SELECT * FROM test_data" should run
