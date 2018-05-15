#!/usr/bin/env bash

# Author: Ilan P

# this scripts gen an .svg file to see clearly the code flow execution for profiling
# between imports

# HOW TO USE
# ============
# just provide the project name which you want to create a execution graph . example " ./gen_profiling.sh CCBR_SAND"
# the svg file will be in the folder project folder DO NOT PUSH IT TO THE GIT



cd .. && python -m cProfile -o 1.stats ~/dev/trax_ace_factory/Projects/$1/Calculations.py -e prod -c ~/dev/theGarageForPS/Trax/Apps/Services/KEngine/k-engine-prod.config
gprof2dot -f pstats 1.stats -o 1.dot
dot -Tsvg -Gdpi=70 -o $1_profiling.svg 1.dot