#!/bin/bash

args=()

for arg in "$@"; do
    if [[ $arg = *"Bdynamic"* ]]; then
        args+=() # we do not want this arg
    elif [[ $arg = *"crti.o"* ]]; then
        args+=("$arg" "-Bstatic")
    elif [[ $arg = *"crtn.o"* ]]; then
        args+=("-lgcc" "-lgcc_eh" "-lc" "/usr/local/lib/gcc/${MUSL_TARGET}/9.4.0/crtendS.o" "$arg")
    else
        args+=("$arg")
    fi
done

echo "RUNNING WITH ARGS: ${args[@]}"
mold -run /usr/local/bin/${MUSL_TARGET}-g++ "${args[@]}"
