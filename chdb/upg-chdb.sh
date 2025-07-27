#!/bin/bash
set -e

echo 'upgrading chDB...'

source ./chgd/py-chdb/bin/activate

python3 -m pip install --upgrade chdb > /dev/null

deactivate