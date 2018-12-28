#!/bin/bash
python -m virtualenv env
source ./env/bin/activate
pip install pip==9.0.3
export PATH="${PATH}:/usr/pgsql-9.2/bin/"
pip install -r requirements.txt