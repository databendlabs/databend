#!/bin/bash -e

# from: https://github.com/ClickHouse/ClickBench/blob/main/generate-results.sh

(
    sed '/^[ \t]*const data = \[$/q' index.html

    FIRST=1
    find ./results/ -name '*.json' | while read -r file; do
        [[ $file =~ ^(hardware|versions)/ ]] && continue

        [ "${FIRST}" = "0" ] && echo -n ','
        jq --compact-output ". += {\"source\": \"${file}\"}" "${file}"
        FIRST=0
    done

    echo ']; // end of data'
    sed '0,/^[ \t]*\]; \/\/ end of data$/d' index.html

) >results/index.html
