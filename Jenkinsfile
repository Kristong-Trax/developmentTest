pipeline {
  agent any
  stages {
    stage('perp') {
      steps {
        dir(path: 'kpi_factory') {
          git(url: 'git@bitbucket.org:traxtechnology/kpi_factory.git', branch: 'master', changelog: true, poll: true)
        }

        dir(path: 'sdk_factory') {
          git(url: 'git@bitbucket.org:traxtechnology/sdk_factory.git', branch: 'master', changelog: true, poll: true)
        }

        dir(path: 'theGarage') {
          git(url: 'https://ilanpinto@bitbucket.org/traxtechnology/trax_dev_garage.git', branch: 'k_engine_dev', changelog: true, credentialsId: 'ilan')
        }

      }
    }

    stage('Tests') {
      parallel {
        stage('unit test') {
          steps {
            sh '''#!/bin/bash
        ## set testing env

        theGarage/Trax/Deployment/conda/DockerMinicondaInstallation.sh

        export PATH="$HOME/miniconda/bin:$PATH"
        conda config --add channels https://conda.anaconda.org/trax
        export PROJECT_ROOT=$PWD/theGarage
        export PYTHONPATH=:$PWD/theGarage



        CONDA=$HOME/miniconda/bin/conda
        dependencies_file=$PROJECT_ROOT/Trax/Deployment/conda/garage.yml
        environment=garage
        recreate_environment=0
        environment_exists=$(${CONDA} info --envs | grep ${environment} | wc -l)

        if [ ${recreate_environment} -eq 1 ]
        then
            echo "Removing old conda $environment environment"
            ${CONDA} env remove -n ${environment}
        fi

        if [ ${environment_exists} -eq 1 ]
        then
            echo "Updating $environment environment"
            ${CONDA} env update -n ${environment} -f ${dependencies_file}
        else
            echo "Creating $environment environment"
            ${CONDA} env create -n ${environment} -f ${dependencies_file}
        fi


        source /var/jenkins_home/miniconda/bin/activate garage
        xargs apt-get install -y --fix-missing < /root/dev/theGarage/Trax/Deployment/conda/requirements.txt
        export PYTHONPATH=:$PWD/theGarage:$PWD/sdk_factory:$PWD/kpi_factory
        pip install awscli
        eval $(aws ecr get-login --no-include-email)'''
          }
        }
        stage('KPI Util Unit Test') {
          steps {
            sh '''#!/bin/bash
        source /var/jenkins_home/miniconda/bin/activate garage
        export PYTHONPATH=:$PWD/theGarage:$PWD/sdk_factory:$PWD/kpi_factory
        python -m unittest discover -s sdk_factory/KPIUtils/Calculations/tests'''
          }
        }
        stage('Projects Unit Test') {
          steps {
            sh '''#!/bin/bash
        source /var/jenkins_home/miniconda/bin/activate garage
        export PYTHONPATH=:$PWD/theGarage:$PWD/sdk_factory:$PWD/kpi_factory
        python -m unittest discover -s kpi_factory/Projects '''
          }
        }
        stage('Projects Regression') {
        when { branch "master" }
           steps {
            sh '''#!/bin/bash
        source /var/jenkins_home/miniconda/bin/activate garage
        export PYTHONPATH=:$PWD/theGarage:$PWD/sdk_factory:$PWD/kpi_factory
        python -m unittest discover -s kpi_factory/Tests'''
          }
         }

	  stage('Check Imports') {
		steps {
			sh '''#!/bin/bash
			source /var/jenkins_home/miniconda/bin/activate garage
			export PYTHONPATH=:$PWD/theGarage:$PWD/sdk_factory:$PWD/kpi_factory
			export PROJECT=$PWD/kpi_factory/Projects
			pwd
			cd kpi_factory/Deployment/Jenkins
			python ImportValidator.py --projects_path $PROJECT '''
		  }
		}

       stage('Static Code Analsys') {
        steps {
         dir('kpi_factory/DevloperTools') {

        sh '''#!/bin/bash
            pip install -r requirements.txt
        '''
        }

		echo getChangeString()
			dir('kpi_factory') {
			echo codeLint()
			}



		}
	   }

	   }
	 }
  }

 environment {
    owenr_mail = 'ilanp@traxretail.com'
    team = 'ps_sw_team@Trax-Tech.com'
    mail_from = 'TraxReports@traxretail.com'
  }


  post{
	  always {
          slackSend color: 'good', message: "  ${JOB_NAME} - ${BUILD_NUMBER} completed successfully . co log: ${env.BUILD_URL}"

		  sh'''#!/bin/bash
            # fix docker issue
            docker stop test_mysql_test_project_1
            docker rm test_mysql_test_project_1
            rm Miniconda-latest-Linux-x86_64.*
            '''
     	}

       failure {
		sendFailMail()

       }

    }


  options {
    timeout(unit: 'MINUTES', time: 20)
  }
}



@NonCPS
def getChangeString() {
 MAX_MSG_LEN = 100
 def changeString = ""

 echo "Gathering SCM changes"
 def changeLogSets = currentBuild.changeSets
 for (int i = 0; i < changeLogSets.size(); i++) {
 def entries = changeLogSets[i].items
 for (int j = 0; j < entries.length; j++) {
 def entry = entries[j]
 truncated_msg = entry.msg.take(MAX_MSG_LEN)
 changeString += " - ${truncated_msg} [${entry.author}]\n"
 }
 }

 if (!changeString) {
 changeString = " - No new changes"
 }
 return changeString
}


def codeLint() {
        def change_log_py = sh (returnStdout: true, script:'''#!/bin/bash
                            git log --name-only --oneline  HEAD^..HEAD | grep .py || true''')
        echo "${change_log_py}"

        if (change_log_py){
            def email_git = sh (returnStdout: true, script:'''#!/bin/bash
                                                         git log --pretty=format:"%ce" HEAD^..HEAD || true
                                                          ''')

            def email = email_git.split()[0]
             echo "${email}"
            def score = sh (returnStdout: true, script:'''#!/bin/bash
            git log --name-only --oneline  HEAD^..HEAD | grep .py | xargs ~/miniconda/envs/garage/bin/pylint > lint.txt  || true
            git log --name-only --oneline  HEAD^..HEAD | xargs ~/miniconda/envs/garage/bin/pylint | grep 'Your code has been rated at' | cut -b 29-35 |cut -d'/' -f1

            ''')

            echo "pylint score is "
            echo "${score}"

                    if ("${score}" && Float.parseFloat("${score}") < 7) {

                         echo "pylint score is "
                         echo "${score}"

                         emailext attachmentsPattern: "**/lint.txt",
                            body: "shame of code is attached to the mail",
                            from: "${mail_from}",
                            subject: "pipeline ${JOB_NAME}-${BUILD_NUMBER} unqualified code",
                            to:"${email} , ${owenr_mail}" // to: "${email} , ${owenr_mail}"
                                  }
		}
}


def sendFailMail(){
	def branch = sh (returnStdout: true, script:'''#!/bin/bash
                                                         git rev-parse --abbrev-ref HEAD || true
                                                          ''')

	if (branch != "master") {
            def email_git = sh (returnStdout: true, script:'''#!/bin/bash
                                                         git log --pretty=format:"%ce" HEAD^..HEAD || true
                                                          ''')
            def email = email_git.split()[0]

			echo "sending mail to ${email}"

			mail body: "${JOB_NAME} build failed maybe it is your code ? /n plz check the log: error log: ${env.BUILD_URL}" ,
            from: "${mail_from}",
            subject: "[action required] build failed!! pipeline ${JOB_NAME} - ${BUILD_NUMBER}",
            to: "${email}"
            error "${err}"


	}
	else {
			mail body: "${JOB_NAME} build failed maybe it is your code ? /n plz check the log: error log: ${env.BUILD_URL}" ,
            from: "${mail_from}",
            subject: "[action required] build failed!! pipeline ${JOB_NAME} - ${BUILD_NUMBER}",
            to: "${team}"
            error "${err}"

		}


}