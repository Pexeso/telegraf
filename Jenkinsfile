node('!win') {
    def registryName = 'pexae.azurecr.io' 
    def buildImageName = "${registryName}/ae-build:1.0"

    stage 'checkout'
    def scmInfo = checkout(scm)

    stage 'docker'
    withCredentials([azureServicePrincipal('ae-jenkins')]) {
        sh 'docker login -u $AZURE_CLIENT_ID -p $AZURE_CLIENT_SECRET pexae.azurecr.io'
    }

    // Get the user & group IDs of the Jenkins users running this pipeline.
    def uid = sh(script: 'id -u', returnStdout: true)?.trim()
    def gid = sh(script: 'id -g', returnStdout: true)?.trim()

    docker.image(buildImageName).inside("--user root") {
        try {
            stage 'build'
            sh 'make'
            sh 'mkdir -p bin && mv telegraf bin/telegraf'
        }
        finally {
            // We build the code as the root user within the container.
            // This 'bin' directory is created in a volume mapped to the
            // Jenkins workspace. To allow the Jenkins user to clean the
            // workspace on the next run, we need to change ownership of
            // the 'bin' directory.
            sh "test -d bin && chown -R ${uid}:${gid} bin"
        }
    }

    // If this commit has a tag associated with it, build new images for the services.
    if (env.TAG_NAME) {
        stage 'deploy'

        docker.build("${registryName}/fdb-telegraf:${env.TAG_NAME}",
            '-f container/Dockerfile bin'
        ).push()
    }
}
