#!/bin/sh

set -Ceu

cat > __init__.py <<EOF
from weakprng import *
EOF
