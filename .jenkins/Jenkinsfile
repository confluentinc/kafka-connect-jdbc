#!/usr/bin/groovy

pipeline {
  agent {
    kubernetes {
      defaultContainer 'maven'
      yaml '''
apiVersion: v1
kind: Pod
spec:
  serviceAccountName: jenkins-codeartifact
  containers:
  - name: maven
    image: maven:3.8.1-jdk-11
    command:
    - cat
    tty: true
  - name: aws
    image: amazon/aws-cli:2.2.18
    command:
    - cat
    tty: true
'''
    }
  }

  stages {
    stage('init') {
      steps {
        withFolderProperties {
          writeFile file: 'settings.xml', text: env.MAVEN_SETTINGS
          container('aws') {
            withAWS(role: env.AWS_CODEARTIFACT_ASSUME_ROLE, useNode: true) {
              script {
                env.CODEARTIFACT_AUTH_TOKEN = sh(script:"aws codeartifact get-authorization-token --domain emnify --domain-owner ${env.AWS_CODEARTIFACT_OWNER} --query authorizationToken --output text", returnStdout: true)
              }
            }
          }
        }
      }
    }
    stage('Build and test') {
      steps {
        sh 'mvn test -q -B -s settings.xml'
      }
    }
    stage('Deploy') {
      when { branch 'master' }
      steps {
        sh 'mvn deploy -q -B -s settings.xml -DskipTests=true'
      }
    }
  }
}