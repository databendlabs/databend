#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

echo "Building docs"
cd website

# NOET: for aarch64 macos
# arch -x86_64 zsh

mkdir -p ~/.nvm
export NVM_DIR="$HOME/.nvm"
# This loads nvm
[ -s "/opt/homebrew/opt/nvm/nvm.sh" ] && \. "/opt/homebrew/opt/nvm/nvm.sh"
[ -s ~/.nvm/nvm.sh ] && source ~/.nvm/nvm.sh

nvm install
nvm use

yarn install
yarn start
