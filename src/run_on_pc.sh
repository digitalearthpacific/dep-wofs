#!/usr/bin/env bash

pip install "git+https://github.com/digitalearthpacific/dep-tools.git@ce43bd9"
pip install "git+https://github.com/jessjaco/azure-logger.git"
python "$@"
