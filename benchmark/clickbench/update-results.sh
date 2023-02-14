#!/bin/bash -e

# from: https://github.com/ClickHouse/ClickBench/blob/main/generate-results.sh

(
    sed '/^const data = \[$/q' index.html

    FIRST=1
    ls -1 ./results/*/result.json | while read file; do
        [[ $file =~ ^(hardware|versions)/ ]] && continue

        [ "${FIRST}" = "0" ] && echo -n ','
        jq --compact-output ". += {\"source\": \"${file}\"}" "${file}"
        FIRST=0
    done

    echo ']; // end of data'
    sed '0,/^\]; \/\/ end of data$/d' index.html

) >results/index.html
