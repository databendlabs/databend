<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# OrbStack as a docker alternative on macOS
1. Install OrbStack by downloading [installer](https://orbstack.dev/download) or using Homebrew.
    ```shell
    brew install orbstack
    ```
   
2. Migrate Docker data
    ```shell
    orb migrate docker
    ```
   
3. (Optional) Add registry mirrors
   
    You can edit the config directly at `~/.orbstack/config/docker.json` and restart the engine with `orb restart docker`.
   
    ```
    {
        "registry-mirrors": ["<mirror_addr>"]
    }
   ```
