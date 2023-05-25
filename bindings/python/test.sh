#!/bin/bash

set -e


python -m venv venv
source venv/bin/activate
pip install maturin
pip install behave
maturin develop

behave tests