#!/bin/sh
source ./venv/Scripts/activate

rm -f ./result/*.csv

python task_duration.py
python app_duration_pd.py
