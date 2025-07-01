// swift-tools-version:6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "swift-jobs-redis",
    platforms: [.macOS(.v15), .iOS(.v18), .tvOS(.v18)],
    products: [
        .library(name: "JobsRedis", targets: ["JobsRedis"])
    ],
    dependencies: [
        .package(url: "https://github.com/hummingbird-project/swift-jobs.git", from: "1.0.0-rc"),
        .package(url: "https://github.com/swift-server/RediStack.git", from: "1.6.2"),
    ],
    targets: [
        .target(
            name: "JobsRedis",
            dependencies: [
                .product(name: "Jobs", package: "swift-jobs"),
                .product(name: "RediStack", package: "RediStack"),
            ]
        ),
        .testTarget(
            name: "JobsRedisTests",
            dependencies: [
                .byName(name: "JobsRedis"),
                .product(name: "Jobs", package: "swift-jobs"),
            ]
        ),
    ]
)
