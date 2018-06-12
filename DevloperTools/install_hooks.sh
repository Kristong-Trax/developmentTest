#!/bin/bash

# update packages
conda install graphviz

pip install -r requirements.txt

# remove pre-commit
KPI_HOME="$(dirname "$PWD")"

if [ -f  $KPI_HOME/.git/hooks/pre-commit ]; then
    mv $KPI_HOME/.git/hooks/pre-commit $KPI_HOME/.git/hooks/pre-commit.old
fi

echo $KPI_HOME

cp ./data/.pylintrc $KPI_HOME
cp ./data/post-merge $KPI_HOME/.git/hooks/post-merge

cp ./data/.pylintrc $KPI_HOME
cp ./data/pre-push $KPI_HOME/.git/hooks/pre-push

chmod +x $KPI_HOME/.git/hooks/post-merge
chmod +x $KPI_HOME/.git/hooks/pre-push

export message='"message"'
export severity='"severity"'
export application='"application"'
export environment='"environment"'
export my_user=$(whoami)

curl -X POST https://logs-01.loggly.com/inputs/2cce0ddd-ce82-4f1f-af5d-f72be7fc67ae/tag/python,PS,Install_hooks/ -d "{$message: '$my_user installed hooks', $severity: 'info', $application: 'PS_dev_tools' , $environment: 'dev'}"
