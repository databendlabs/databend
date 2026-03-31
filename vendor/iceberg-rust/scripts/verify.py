#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import subprocess
import sys
import os

BASE_DIR = os.getcwd()


def check_rust():
    try:
        subprocess.run(["cargo", "--version"], check=True)
        return True
    except FileNotFoundError:
        return False
    except Exception as e:
        raise Exception("Check rust met unexpected error", e)

def build_core():
    print("Start building iceberg rust")

    subprocess.run(["cargo", "build", "--release"], check=True)

def main():
    if not check_rust():
        print(
            "Cargo is not found, please check if rust development has been setup correctly"
        )
        print("Visit https://www.rust-lang.org/tools/install for more information")
        sys.exit(1)

    build_core()

if __name__ == "__main__":
    main()