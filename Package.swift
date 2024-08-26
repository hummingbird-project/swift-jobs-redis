// swift-tools-version:5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "swift-jobs-redis",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "JobsRedis", targets: ["JobsRedis"]),
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/swift-jobs.git", branch: "job-scheduler"),
        .package(url: "https://github.com/swift-server/RediStack.git", from: "1.4.0"),
    ],
    targets: [
        .target(name: "JobsRedis", dependencies: [
            .product(name: "Jobs", package: "swift-jobs"),
            .product(name: "RediStack", package: "RediStack"),
        ]),
        .testTarget(name: "JobsRedisTests", dependencies: [
            .byName(name: "JobsRedis"),
            .product(name: "Jobs", package: "swift-jobs"),
        ]),
    ]
)
