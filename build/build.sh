#!/usr/bin/env bash

set -e
set -o pipefail

ROOT=`git rev-parse --show-toplevel`
OUT_DIR="$ROOT/out"
VENV_NAME="venv"
PYTHON=`which python3`
REQUIREMENTS="$ROOT/requirements.txt"

echo "----- Configuration -----"
echo "RootDir:  $ROOT"
echo "OutDir:   $OUT_DIR"
echo "Python:   $PYTHON"

mkdir -p $OUT_DIR
pushd $OUT_DIR

# Make the venv
$PYTHON -m virtualenv $VENV_NAME

# Activate the venv
. $OUT_DIR/$VENV_NAME/bin/activate

# Install the requirements
python -m pip install -r $REQUIREMENTS

# Deactivate VENV
deactivate