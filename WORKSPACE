workspace(name = "com_datadoghq_azure_log_forwarding_offering")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "rules_oci_bootstrap",
    commit = "f8212e495664a2e59358cf930411637a7fe303f5",
    remote = "https://github.com/DataDog/rules_oci_bootstrap.git",
)

load("@rules_oci_bootstrap//:defs.bzl", "oci_blob_pull")
load("//rules/cnab:version.bzl", "CNAB_TOOLS_VERSION")

oci_blob_pull(
    name = "com_datadoghq_cnab_tools",
    digest = CNAB_TOOLS_VERSION,
    extract = True,
    registry = "registry.ddbuild.io",
    repository = "cnab-tools/rules_cnab",
    type = "tar.gz",
)

load("@com_datadoghq_cnab_tools//rules:deps.bzl", "cnab_tools_dependencies")

cnab_tools_dependencies()

load("//:create_git_repository_with_metadata.bzl", "git_repository_with_commit_meta")

git_repository_with_commit_meta(
    name = "com_datadoghq_datacenter_config",
    commit = "8fdcec96b0a96c53ac57b43fbc97587687546221",
    filename = "dc_config_version",
    remote = "https://github.com/DataDog/datacenter-config.git",
)

load("@com_datadoghq_datacenter_config//rules:deps.bzl", "datacenter_config_dependencies")

datacenter_config_dependencies()

