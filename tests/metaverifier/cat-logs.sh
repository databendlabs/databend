#!/bin/bash

dir_path="/.databend/logs"

for file in "$dir_path"/*
do
    if [ -f "$file" ]; then
        echo "\n=== Contents of $file ==="
        cat "$file"
        echo "=== End of $file ===\n"
    fi
done