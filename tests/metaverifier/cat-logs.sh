#!/bin/bash

dir_path="/.databend"

for file in "$dir_path"/*
do
    if [ -f "$file" ]; then
        echo "=== Contents of $file ==="
        cat "$file"
        echo "=== End of $file ==="
    fi
done