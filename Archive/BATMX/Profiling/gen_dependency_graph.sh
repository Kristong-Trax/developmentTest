

#!/usr/bin/env bash

__author__ = '%(author)s'

# this scripts gen an .svg file to see clearly the dependencies
# between imports

# HOW TO USE
# ============
# cd to kpi_factory/Projects/<your project>/Profiling
# in terminal : ./gen_dependency_graph.sh

PROJECT_DIR=$PWD


PARENT_DIR="$(dirname "$PROJECT_DIR")"



~/miniconda/envs/garage/bin/sfood ${PARENT_DIR}/ | ~/miniconda/envs/garage/bin/sfood-graph > /tmp/d.dot
dot -Tsvg -Gdpi=70 /tmp/d.dot -o ${PROJECT_DIR}/graph1.svg



export message='"message"'
export severity='"severity"'
export application='"application"'
export environment='"environment"'
export my_user=$(whoami)

curl -X POST https://logs-01.loggly.com/inputs/2cce0ddd-ce82-4f1f-af5d-f72be7fc67ae/tag/python,PS,Install_hooks/ -d "{action: gen dependency graph, $severity: 'info', $application: 'PS_dev_tools' , $environment: 'dev', user:$my_user}"

