#!/bin/bash

set -e

pegasus-plan \
    --conf pegasus.conf \
    --dax Test.dax \
    --dir /nas/gaia/users/hhasan/Projects/vista-pegasus-wrapper/experiments/working \
    --cleanup leaf \
    --force \
    --sites saga \
    --output-site local \
    --submit
