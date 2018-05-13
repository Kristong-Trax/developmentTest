#!/bin/bash

# update packages
pip install -r requirements.txt
conda install graphviz

# remove pre-commit
FACTORY_HOME="$(dirname "$PWD")"

if [ -f  $FACTORY_HOME/.git/hooks/pre-commit ]; then
    mv $FACTORY_HOME/.git/hooks/pre-commit $FACTORY_HOME/.git/hooks/pre-commit.old
fi


cp ./data/.pylintrc $FACTORY_HOME
cp ./data/pre-push $FACTORY_HOME/.git/hooks/pre-push

cp ./data/.pylintrc $FACTORY_HOME
cp ./data/post-merge $FACTORY_HOME/.git/hooks/post-merge

chmod +x $FACTORY_HOME/.git/hooks/pre-push
chmod +x $FACTORY_HOME/.git/hooks/post-merge

export message='"message"'
export severity='"severity"'
export application='"application"'
export environment='"environment"'
export my_user=$(whoami)

curl -X POST https://logs-01.loggly.com/inputs/2cce0ddd-ce82-4f1f-af5d-f72be7fc67ae/tag/python,PS,Install_hooks/ -d "{$message: '$my_user installed hooks', $severity: 'info', $application: 'PS_dev_tools' , $environment: 'dev'}"
