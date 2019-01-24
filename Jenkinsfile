// This Jenkinsfile uses the declarative syntax. If you need help, check:
// Overview and structure: https://jenkins.io/doc/book/pipeline/syntax/
// For plugins and steps:  https://jenkins.io/doc/pipeline/steps/
// For Github integration: https://github.com/jenkinsci/pipeline-github-plugin
// For credentials:        https://jenkins.io/doc/book/pipeline/jenkinsfile/#handling-credentials
// For credential IDs:     https://ci.ts.sv/credentials/store/system/domain/_/
// Docker:                 https://jenkins.io/doc/book/pipeline/docker/
// Custom commands:        https://github.com/Tradeshift/jenkinsfile-common/tree/master/vars
// Environment variables:  env.VARIABLE_NAME

pipeline {
    agent any // Or you could make the whole job or certain stages run inside docker
    options {
        ansiColor('xterm')
        timestamps()
    }
    tools {
       jdk 'oracle-java8u162-jdk'
       maven 'apache-maven-3.5.0'
    }
    triggers {
        issueCommentTrigger('retest')
    }
    environment {
        P12_PASSWORD = credentials 'client-cert-password'
        MAVEN_OPTS = "-Djavax.net.ssl.keyStore=/var/lib/jenkins/.m2/certs/jenkins.p12 -Djavax.net.ssl.keyStoreType=pkcs12 -Djavax.net.ssl.keyStorePassword=$P12_PASSWORD"
    }

    stages {
        stage('Initialise PR') {
            when { changeRequest() }
            steps {
                // We need to reset the SonarQube status in the beginning
                githubNotify(status: 'PENDING', context: 'sonarqube', description: 'Not analysed')
            }
        }

        stage('Clone') {
            steps {
                checkout scm
            }
        }

        stage('Compile') {
            when {
                changeRequest()
            }
            steps {
                sh 'mvn compile'
            }
        }

        stage('Test') {
            when {
                changeRequest()
            }
            steps {
                sh 'mvn test'
            }
        }

        stage('Deploy') {
            environment {
               IMAGE_VERSION = "${env.CHANGE_ID ? 'ghprb-' : ''}${env.GIT_COMMIT}"
               IMAGE = "docker.tradeshift.net/tradeshift-kafka-connect-jdbc:$IMAGE_VERSION"
            }
            steps {
                sh 'mvn clean package -DskipTests && docker build -t $IMAGE . && docker push $IMAGE'
                script {
                    if (env.CHANGE_ID) {
                        pullRequest.comment("Pushed docker image: `$IMAGE`")
                    }
                }
            }
        }
    }
    
}