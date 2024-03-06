workspace(name = "com_datadoghq_azure_log_forwarding_offering")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "com_datadoghq_cnab_tools",
    commit = "REPLACE_WITH_LATEST_COMMIT",
    remote = "https://github.com/DataDog/cnab-tools.git",
)

load("@com_datadoghq_cnab_tools//rules:deps.bzl", "cnab_tools_dependencies")
cnab_tools_dependencies()

git_repository(
    name = "com_datadoghq_datacenter_config",
    commit = "REPLACE_WITH_LATEST_COMMIT",
    remote = "https://github.com/DataDog/datacenter-config.git",
)

load("@com_datadoghq_datacenter_config//rules:deps.bzl", "datacenter_config_dependencies")
datacenter_config_dependencies()

load("@com_datadoghq_datacenter_config//rules/setup:rules_go.bzl", "rules_go_setup")
rules_go_setup()

load("@com_datadoghq_datacenter_config//rules/setup:rules_docker.bzl", "rules_docker_setup")
rules_docker_setup()

load("@com_datadoghq_datacenter_config//rules/setup:gazelle.bzl", "gazelle_setup")
gazelle_setup()