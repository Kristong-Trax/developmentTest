#!/usr/bin/env bash

# Author: ilan p

# this scripts gen an .svg file to see clearly the dependencies
# between imports

# HOW TO USE
# ============
# just provide the folder which you want to create a dep graph . example "./gen_dependency_graph.sh /home/Ilan/dev/trax_ace_factory/KPIUtils"
# the svg file will be in the folder /home/Ilan/dev/trax_ace_factory/KPIUtils


if [ -z $1 ] ; then
    echo "plz provide a folder"
    exit 1
fi

~/miniconda/envs/garage/bin/sfood $1/ | ~/miniconda/envs/garage/bin/sfood-graph > /tmp/d.dot
dot -Tsvg -Gdpi=70 /tmp/d.dot -o $1/graph1.svg



export message='"message"'
export severity='"severity"'
export application='"application"'
export environment='"environment"'
export my_user=$(whoami)

curl -X POST https://logs-01.loggly.com/inputs/2cce0ddd-ce82-4f1f-af5d-f72be7fc67ae/tag/python,PS,Install_hooks/ -d "{action: gen dependency graph, $severity: 'info', $application: 'PS_dev_tools' , $environment: 'dev', user:$my_user}"
