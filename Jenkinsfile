pipeline {
    agent {
        kubernetes {
            label 'python-stretch'
        }
    }
    stages {
        stage('Prepare') {
            steps {
                script {
                    env.GIT_COMMIT = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
                }
                container('vault') {
                    script {
                        env.PYPI_USERNAME = sh(script: 'vault read -field=username secret/ops/account/pypi', returnStdout: true)
                        env.PYPI_PASSWORD = sh(script: 'vault read -field=password secret/ops/account/pypi', returnStdout: true)
                        env.TESTPYPI_USERNAME = sh(script: 'vault read -field=username secret/ops/account/testpypi', returnStdout: true)
                        env.TESTPYPI_PASSWORD = sh(script: 'vault read -field=password secret/ops/account/testpypi', returnStdout: true)
                        env.GITHUB_TOKEN = sh(script: 'vault read -field=value secret/ops/token/github', returnStdout: true)
                        env.SONAR_TOKEN = sh(script: 'vault read -field=value secret/ops/token/sonar', returnStdout: true)
                    }
                }
                container('python') {
                    script {
                        sh "pip install tox"
                    }
                }
            }
        }
        stage('Build: [ pull request ]') {
            when {
                changeRequest()
            }
            steps {
                container('python') {
                    sh "tox"
                }
                container('sonar') {
                    sh "sonar-scanner -Dsonar.github.oauth=${env.GITHUB_TOKEN} -Dsonar.pullrequest.base=${CHANGE_TARGET} -Dsonar.pullrequest.branch=${BRANCH_NAME} -Dsonar.pullrequest.key=${env.CHANGE_ID} -Dsonar.pullrequest.provider=GitHub -Dsonar.pullrequest.github.repository=molgenis/molgenis-py-bbmri-eric"
                }
            }
        }
        stage('Build: [ main ]') {
            when {
                branch 'main'
            }
            steps {
                milestone 1
                container('python') {
                    sh "tox"
                }
                container('sonar') {
                    sh "sonar-scanner"
                }
            }
        }
        stage('Release: [ main ]') {
            when {
                branch 'main'
            }
            environment {
                REPOSITORY = 'molgenis/molgenis-py-bbmri-eric'
            }
            steps {
                timeout(time: 15, unit: 'MINUTES') {
                    script {
                        env.RELEASE_SCOPE = input(
                                message: 'Do you want to release?',
                                ok: 'Release to TestPyPi',
                                parameters: [
                                        choice(choices: 'patch\nminor\nmajor', description: '', name: 'RELEASE_SCOPE')
                                ]
                        )
                    }
                }
                milestone 2
                container('python') {
                    sh "git remote set-url origin https://${GITHUB_TOKEN}@github.com/${REPOSITORY}.git"

                    sh "git checkout -f main"

                    script {
                        env.CURRENT_PACKAGE_VERSION = sh(script: "git tag | sort -r --version-sort | head -n1", returnStdout: true).trim()
                    }

                    sh "pip install bumpversion"

                    script {
                        env.NEW_PACKAGE_VERSION = sh(script: "bumpversion --current-version ${CURRENT_PACKAGE_VERSION} --list ${RELEASE_SCOPE} | grep new_version= | cut -d'=' -f2", returnStdout: true).trim()
                    }

                    sh "tox -e build"

                    sh "tox -e publish -- --repository testpypi --username ${TESTPYPI_USERNAME} --password ${TESTPYPI_PASSWORD}"

                    sh "git push --tags origin main"
                }
                timeout(time: 60, unit: 'MINUTES') {
                    script {
                        env.RELEASE_SCOPE = input(
                                message: 'Continue releasing if the release to TestPyPi was successful (https://test.pypi.org/project/molgenis-py-bbmri-eric/${NEW_PACKAGE_VERSION}/), abort otherwise.',
                                ok: 'Release to PyPi'
                        )
                    }
                }
                container('python') {
                    sh "tox -e publish -- --repository pypi --username ${PYPI_USERNAME} --password ${PYPI_PASSWORD}"

                    hubotSend(message: "molgenis-py-bbmri-eric ${NEW_PACKAGE_VERSION} has been released! :tada: https://pypi.org/project/molgenis-py-bbmri-eric/", status:'SUCCESS')
                }
            }
        }
    }
    post{
        success {
            notifySuccess()
        }
        failure {
            notifyFailed()
        }
    }
}

def notifySuccess() {
    hubotSend(message: 'Build success', status:'INFO', site: 'slack-pr-app-team')
}

def notifyFailed() {
    hubotSend(message: 'Build failed', status:'ERROR', site: 'slack-pr-app-team')
}
