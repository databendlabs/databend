#!/bin/bash -e

# from: https://github.com/ClickHouse/ClickBench/blob/main/generate-results.sh

dataset=$1

if [ -z "$dataset" ]; then
    echo "Usage: $0 <dataset>"
    exit 1
fi

(
    sed '/^[ \t]*const data = \[$/q' "${dataset}/index.html"

    FIRST=1
    find "./results/${dataset}/" -name '*.json' | while read -r file; do
        [[ $file =~ ^(hardware|versions)/ ]] && continue

        [ "${FIRST}" = "0" ] && echo -n ','
        jq --compact-output ". += {\"source\": \"${file}\"}" "${file}"
        FIRST=0
    done

    echo ']; // end of data'
    sed '0,/^[ \t]*\]; \/\/ end of data$/d' "${dataset}/index.html"

) >"results/${dataset}.html"
