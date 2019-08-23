load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@bazel_gazelle//:def.bzl", "gazelle")

# gazelle:prefix github.com/droyo/stackdriver-profiler-perf
gazelle(name = "gazelle")

go_library(
    name = "go_default_library",
    srcs = ["main.go"],
    importpath = "github.com/droyo/stackdriver-profiler-perf",
    visibility = ["//visibility:private"],
    deps = [
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@go_googleapis//google/devtools/cloudprofiler/v2:cloudprofiler_go_proto",
        "@go_googleapis//google/rpc:errdetails_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//credentials:go_default_library",
        "@org_golang_google_grpc//credentials/oauth:go_default_library",
        "@org_golang_google_grpc//metadata:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)

go_binary(
    name = "sd_perf_agent",
    embed = [":go_default_library"],
    visibility = ["//visibility:public"],
)
