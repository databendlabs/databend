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

Feature: Databend Driver

    Scenario: Select Simple
        Given A new Databend Driver Client
        Then Select string "Hello, Databend!" should be equal to "Hello, Databend!"

    Scenario: Select Types
        Given A new Databend Driver Client
        Then Select types should be expected native types

    Scenario: Select Iter
        Given A new Databend Driver Client
        Then Select numbers should iterate all rows

    Scenario: Insert and Select
        Given A new Databend Driver Client
        When Create a test table
        Then Insert and Select should be equal

    Scenario: Stream Load
        Given A new Databend Driver Client
        When Create a test table
        Then Stream load and Select should be equal
