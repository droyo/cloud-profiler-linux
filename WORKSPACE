load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "io_bazel_rules_go",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/rules_go/releases/download/0.19.1/rules_go-0.19.1.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/0.19.1/rules_go-0.19.1.tar.gz",
    ],
    sha256 = "8df59f11fb697743cbb3f26cfb8750395f30471e9eabde0d174c3aebc7a1cd39",
)

http_archive(
    name = "bazel_gazelle",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/bazel-gazelle/releases/download/0.18.1/bazel-gazelle-0.18.1.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/0.18.1/bazel-gazelle-0.18.1.tar.gz",
    ],
    sha256 = "be9296bfd64882e3c08e3283c58fcb461fa6dd3c171764fcc4cf322f60615a9b",
)

load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies", "go_register_toolchains")

go_rules_dependencies()

go_register_toolchains()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

# https://github.com/protocolbuffers/protobuf/pull/5389

http_archive(
    name = "com_google_protobuf",
    sha256 = "8eb5ca331ab8ca0da2baea7fc0607d86c46c80845deca57109a5d637ccb93bb4",
    strip_prefix = "protobuf-3.9.0",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.9.0.zip"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

go_repository(
    name = "org_golang_google_grpc",
    build_file_proto_mode = "disable",
    importpath = "google.golang.org/grpc",
    sum = "h1:J0UbZOIrCAl+fpTOf8YLs4dJo8L/owV4LYVtAXQoPkw=",
    version = "v1.22.0",
)

go_repository(
    name = "org_golang_x_net",
    importpath = "golang.org/x/net",
    sum = "h1:oWX7TPOiFAMXLq8o0ikBYfCJVlRHBcsciT5bXOrH628=",
    version = "v0.0.0-20190311183353-d8887717615a",
)

go_repository(
    name = "org_golang_x_text",
    importpath = "golang.org/x/text",
    sum = "h1:g61tztE5qeGQ89tm6NTjjM9VPIm088od1l6aSorWRWg=",
    version = "v0.3.0",
)

http_archive(
    name = "com_google_pprof",
    build_file = "BUILD.pprof",
    sha256 = "fbc735cf0a147a1792f78a64e2a26f62e016177b74deb657426f5bdebe4f55f3",
    strip_prefix = "pprof-34ac40c",
    urls = ["https://github.com/google/pprof/archive/34ac40c.zip"],
)

http_archive(
    name = "boringssl",
    urls = ["https://github.com/google/boringssl/archive/f682744abb15a47a7075abc8ba78108b9a34a1a0.zip"],
    strip_prefix = "boringssl-f682744abb15a47a7075abc8ba78108b9a34a1a0",
    sha256 = "deb88069f9bdbf0946a67baf6a0f69153c7fc446016bee02b5f8477b101aed07",
)

http_archive(
    name = "com_google_perf_data_converter",
    sha256 = "40f1bb3fc9f2f04eca0dcf5041827f86797c9f7e62b68a7377f61668c075f3cf",
    strip_prefix = "perf_data_converter-6c1f25926799345df918a4a483287427c048aac3",
    urls = ["https://github.com/google/perf_data_converter/archive/6c1f25926799345df918a4a483287427c048aac3.zip"],
)

go_repository(
    name = "org_golang_x_oauth2",
    importpath = "golang.org/x/oauth2",
    sum = "h1:SVwTIAaPC2U/AvvLNZ2a7OVsmBpC8L5BlwK1whH3hm0=",
    version = "v0.0.0-20190604053449-0f29369cfe45",
)

go_repository(
    name = "com_google_cloud_go",
    importpath = "cloud.google.com/go",
    sum = "h1:0BWXxb/yzTc5MjzcLfBceY2xuwawl5cIbCC7qsLuktA=",
    version = "v0.44.0",
)
