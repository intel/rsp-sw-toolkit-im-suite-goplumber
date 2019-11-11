rrpBuildGoCode {
    projectKey = 'goplumber'
    testDependencies = ['mongo']
    dockerBuildOptions = ['--squash', '--build-arg GIT_COMMIT=$GIT_COMMIT']
    ecrRegistry = "280211473891.dkr.ecr.us-west-2.amazonaws.com"
    dockerImageName = "rsp/${projectKey}"
    protexProjectName = 'bb-goplumber'
    buildImage = 'amr-registry.caas.intel.com/rrp/ci-go-build-image:1.12.0-alpine'
    skipDocker = true
    skipBuild = true

    infra = [
        stackName: 'RSP-Codepipeline-GoPlumber'
    ]

    notify = [
        slack: [ success: '#ima-build-success', failure: '#ima-build-failed' ]
    ]
}
