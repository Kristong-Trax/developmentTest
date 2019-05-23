pipeline {
   agent {
        docker {
            label 'ec2-asg-jenkins'
            image '619597279328.dkr.ecr.us-east-1.amazonaws.com/garage:jenkins_slave_base'
            args ' --name kif_test -u root -v /var/run/docker.sock:/var/run/docker.sock -v /dev/log:/dev/log -v \
                  /root/engineCache/TestResults:/root/engineCache/TestResults --log-driver=syslog'
        }
    }
    triggers {
      bitbucketPush()
    }


    options {
      timestamps()
      timeout(time: 5, unit: 'HOURS')
      checkoutToSubdirectory('kpi_factory')
      buildDiscarder(logRotator(numToKeepStr: '50'))
    }
    environment {
        CONDA_BIN = "/root/miniconda/bin"
        FAILED_STAGE = "Init"
        owenr_mail = 'ilanp@traxretail.com'
        TEAM = 'kif@Trax-Tech.com'
        mail_from = 'TraxReports@traxretail.com'



    }

  stages {
            stage('Start') {
            steps {
                bitbucketStatusNotify(buildState: 'INPROGRESS')
                script {
                    env.FAILED_STAGE = "${env.STAGE_NAME}"
                    sh 'printenv'
                    sh 'docker system info'
                    sh '/root/miniconda/bin/conda info'
                    def commiters = emailextrecipients([developers()])

                    echo "${commiters}"
                    def payload = "${env.BITBUCKET_PAYLOAD}"
                    echo "${payload}"
                }
            }
        }
        stage('Build Environment') {
            steps {
                script {
                    env.FAILED_STAGE = "${env.STAGE_NAME}"
                }

               dir(path: 'theGarage') {
                      git(url: 'https://ilanpinto@bitbucket.org/traxtechnology/trax_dev_garage.git', branch: 'master', changelog: true, poll: true, credentialsId: 'ilanp')
                }

                sh '''#!/bin/bash
                    export PATH=$CONDA_BIN:$PATH
                    cd theGarage
                    echo Cleaning repository...
                    git clean -xdf
                    conda env update -n garage -f ./Trax/Deployment/conda/garage.yml
                    exit_code=$?
                    exit $exit_code
                '''
            }
        }

    stage('perp') {
      steps {
        dir(path: 'sdk_factory') {
          git(url: 'https://ilanpinto@bitbucket.org/traxtechnology/sdk_factory.git', branch: 'master', changelog: true, poll: true, credentialsId: 'ilanp')
        }

        dir(path: 'trax_ace_live') {
          git(url: 'https://ilanpinto@bitbucket.org/traxtechnology/trax_ace_live.git', branch: 'master', changelog: true, poll: true, credentialsId: 'ilanp')
        }




        sh '''#!/bin/bash
        ## set testing env
        export PYTHONPATH=:$PWD/theGarage:$PWD/trax_ace_live
        source ~/miniconda/bin/activate garage
        cp -r kpi_factory/Tests trax_ace_live/Tests
        export PROJECT_HOME=$PWD
        docker network create --driver=bridge --subnet=10.3.3.0/24 --ip-range=10.3.3.0/24 --gateway=10.3.3.1 trax_test
        docker network connect trax_test kif_test
        $(aws ecr get-login --region us-east-1 --no-include-email)


                '''

      }
    }

    stage('SDK factory Tests') {
          steps {
            sh '''#!/bin/bash
            source ~/miniconda/bin/activate garage
            $(aws ecr get-login --region us-east-1 --no-include-email)
            export PYTHONPATH=:$PWD/sdk_factory:$PWD/theGarage:$PWD/trax_ace_live:$PWD/OutOfTheBox
            python -m unittest discover -s sdk_factory/KPIUtils/Calculations/tests
            python -m unittest discover -s sdk_factory/KPIUtils_v2/Calculations
            python -m unittest discover -s sdk_factory/OutOfTheBox
        '''

          }
        }




        stage('Factory Projects Unit Test') {

            steps {
                script {
                    env.FAILED_STAGE = "${env.STAGE_NAME}"
                }
                sh '''#!/bin/bash
                    export PYTHONPATH=$PWD
                    export PROJECT_ROOT=$PWD
                    export PYTHONPATH=:$PWD/theGarage:$PWD/kpi_factory:$PWD/sdk_factory
                    export PATH=$CONDA_BIN:$PATH
                    source activate garage
                    $(aws ecr get-login --region us-east-1 --no-include-email)
                    python -u theGarage/Trax/Tools/Testing/Runners/UnitTestsRunner.py -p test_unit_*.py -f ./Trax/Tools/Testing/Log/log.txt -iu=True -tp $PWD/kpi_factory/Projects,$PWD/kpi_factory,$PWD/sdk_factory
                    tests_exit_code=$?
                    echo "Completed running tests with exit value: " $tests_exit_code.
                    exit $tests_exit_code
                '''
            }
            post {
                always {
                    sh 'mv -f ${WORKSPACE}/theGarage/Trax/Tools/Testing/Runners/Results/recent-test-results.html \
                              ${WORKSPACE}/kpi_and_sdk_unit-tests-results.html'
                    archiveArtifacts 'kpi_and_sdk_unit-tests-results.html'
                }
            }
         }

      stage('Factory Projects Functional Test') {
          steps {
            sh '''#!/bin/bash
            docker network rm trax_test
            export PROJECT_ROOT=$PWD
            export PYTHONPATH=:$PWD/theGarage:$PWD/kpi_factory:$PWD/sdk_factory
            source ~/miniconda/bin/activate garage
            $(aws ecr get-login --region us-east-1 --no-include-email)
            docker pull 619597279328.dkr.ecr.us-east-1.amazonaws.com/garage:mongo_3_6
            docker pull 619597279328.dkr.ecr.us-east-1.amazonaws.com/garage:redis_3_0
            docker pull 619597279328.dkr.ecr.us-east-1.amazonaws.com/traxdatabase/db:latest
            python -u theGarage/Trax/Tools/Testing/Runners/UnitTestsRunner.py -p test_functional_*.py -f ./Trax/Tools/Testing/Log/log.txt -iu=True -tp=$PWD/kpi_factory/Projects,$PWD/kpi_factory,$PWD/sdk_factory
            tests_exit_code=$?
            echo "Completed running tests with exit value: " $tests_exit_code.
            exit $tests_exit_code
            '''
          }
          post {
                always {
                    sh 'mv -f ${WORKSPACE}/theGarage/Trax/Tools/Testing/Runners/Results/recent-test-results.html \
                              ${WORKSPACE}/factory_unit-tests-results.html'
                    archiveArtifacts 'factory_unit-tests-results.html'
                }
            }
        }



 }



  post{
     cleanup {
            sh '''#!/bin/bash
                echo "Before cleanup"
                docker images
                docker ps

                cd master
                export PATH=$CONDA_BIN:$PATH
                export PROJECT_ROOT=$PWD
                export PYTHONPATH=$PWD
                source activate garage
                python -u Trax/Deployment/Remote/SlaveCleaner.py

                echo "After cleanup"
                docker images
                docker ps
            '''
            cleanWs()
        }

       success {
            bitbucketStatusNotify(buildState: 'SUCCESSFUL')
            slackSend channel:"#kif_alert", color: "good", message: "  ${JOB_NAME} - ${BUILD_NUMBER} completed successfully . co log: ${env.BUILD_URL}"
        }


       failure {
	   script {
            env.DEPLOY_STATUS = "$currentBuild.currentResult"
            env.START_TIME = "$currentBuild.startTimeInMillis"
            env.DURATION = "$currentBuild.duration"
            sh '''#!/bin/bash
                cd theGarage
                export PATH=$CONDA_BIN:$PATH
                export PYTHONPATH=$PWD
                export PROJECT_ROOT=$PWD
                source activate garage
                git remote set-url origin git@bitbucket.org:traxtechnology/trax_dev_garage.git
                python -u Trax/Deployment/Tools/BuildReport.py --failed_step="$FAILED_STAGE" \
                    --result=$DEPLOY_STATUS --start_time=$START_TIME --duration=$DURATION --job_name=$JOB_NAME \
                    --build_number=$BUILD_DISPLAY_NAME --url=$RUN_DISPLAY_URL
            '''
            }

       }

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

           def committers = emailextrecipients([developers()])


            slackSend channel:"#kif_alert", color: "bad", message: "${JOB_NAME} - ${env.BUILD_NUMBER} completed unsuccessfully committer list: ${committers}.log: ${env.BUILD_URL}"
            slackSend channel:"#ps_alerts", color: "bad", message: "${JOB_NAME} - ${env.BUILD_NUMBER} completed unsuccessfully committer list: ${committers}.log: ${env.BUILD_URL}"
            bitbucketStatusNotify(buildState: 'FAILED')

            emailext (
                to: "ilanp@traxretail.com",
                recipientProviders: [[$class: 'DevelopersRecipientProvider']],
                subject: "[Build Failed] ${JOB_NAME}",
                mimeType: 'text/html',
                attachmentsPattern: '*.html',
                body: '${SCRIPT, template="fail-email.template"}'
            )

}
