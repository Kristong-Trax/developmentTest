

#!/usr/bin/env bash

__author__ = '%(author)s'

# this scripts gen an .svg file to see clearly the code flow execution for profiling
# between imports

# HOW TO USE
# ============
# cd to kpi_factory/Projects/<your project>/Profiling
# in terminal : ./gen_profiling.sh


PROJECT_DIR=$PWD

PARENT_DIR="$(dirname "$PROJECT_DIR")"

PROJECT=${PARENT_DIR##*/}


cd .. && cd .. && cd .. && python -m cProfile -o 1.stats ~/dev/kpi_factory/Projects/$PROJECT/Calculations.py -e prod -c ~/dev/theGarage/Trax/Apps/Services/KEngine/k-engine-prod.config
gprof2dot -f pstats 1.stats -o 1.dot
dot -Tsvg -Gdpi=70 -o ${PROJECT}_profiling.svg 1.dot

mv ~/dev/kpi_factory/1.dot ${PROJECT_DIR}/1.dot
mv ~/dev/kpi_factory/1.stats ${PROJECT_DIR}/1.stats
mv ~/dev/kpi_factory/${PROJECT}_profiling.svg ${PROJECT_DIR}/${PROJECT}_profiling.svg

