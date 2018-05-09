#!/usr/bin/env bash

# Author: Ilan P

# this scripts gen an .svg file to see clearly the code flow execution for profiling
# between imports

# HOW TO USE
# ============
# just provide the project name which you want to create a execution graph . example " ./gen_profiling.sh CCBR_SAND"
# the svg file will be in the folder project folder DO NOT PUSH IT TO THE GIT



cd .. && python -m cProfile -o 1.stats ~/dev/kpi_factory/Projects/$1/Calculations.py -e prod -c ~/dev/theGarage/Trax/Apps/Services/KEngine/k-engine-prod.config
gprof2dot -f pstats 1.stats -o 1.dot
dot -Tsvg -Gdpi=70 -o $1_profiling.svg 1.dot

export message='"message"'
export severity='"severity"'
export application='"application"'
export environment='"environment"'
export my_user=$(whoami)

curl -X POST https://logs-01.loggly.com/inputs/2cce0ddd-ce82-4f1f-af5d-f72be7fc67ae/tag/python,PS,Install_hooks/ -d "{action: gen profile server, $severity: 'info', $application: 'PS_dev_tools' , $environment: 'dev', user:$my_user}"
