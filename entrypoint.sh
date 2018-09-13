#!/bin/sh

if [ ! -d 'geozones.egg-info' ]; then
    pip install -e /src/
fi
geozones "$@"
