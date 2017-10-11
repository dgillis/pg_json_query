#!/usr/bin/env bash

# This script adds the extension to the PostgreSQL installation under the current
# path

set -e
set -o nounset


JSON_QUERY_DIR="`readlink -f $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/`"


if [ -n "${1+isset}" ]; then
    if [ "$1" = "--help" ]; then
        echo "Install the JSON Query PostgreSQL extension. The optional path argument can be used to provide "
        echo "an alternate path to use to locate the pg_config command to use."
        echo ""
        echo "Usage: pg-add-extension.sh {path}"
        exit 1
    fi;
    
    if [ ! -d "$1" ]; then
        echo "No such directory: $1"
        exit 1
    fi;
    
    EXTRA_PATH="$(cd $1 && pwd)"
    export PATH="$EXTRA_PATH:$PATH"
fi

if ! PG_CONFIG="$(which pg_config)"; then
    echo "pg_config not found. Unable to install JSON Query."
    exit 1
fi;


read -r -p  "Install JSON Query extension (pg_config=$PG_CONFIG)? [y/N]" RESP
if [[ ! "$RESP"  =~ ^([yY][eE][sS]|[yY])$ ]]; then
    echo "Cancelled."
    exit 1
fi
# Give a brief chance to kill.
echo "Installing extension... "
sleep 1


if cd "$JSON_QUERY_DIR" && sudo env "PATH=$PATH" make install; then
    echo "done."
    exit 0
else
    echo "install failed."
    echo 1;
fi;
