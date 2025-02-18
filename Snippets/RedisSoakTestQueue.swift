import Jobs
import JobsRedis
import Logging
import NIOCore
import NIOPosix
import RediStack
import ServiceLifecycle
import Synchronization

var logger = Logger(label: "Soak")
logger.logLevel = .debug
let redis = try RedisConnectionPool(
    configuration: .init(
        initialServerConnectionAddresses: [.makeAddressResolvingHost("localhost", port: 6379)],
        maximumConnectionCount: .maximumActiveConnections(2),
        connectionFactoryConfiguration: .init(
            connectionDefaultLogger: logger,
            tcpClient: nil
        ),
        minimumConnectionCount: 0,
        connectionBackoffFactor: 2,
        initialConnectionBackoffDelay: .milliseconds(100)
    ),
    boundEventLoop: MultiThreadedEventLoopGroup.singleton.any()
)

let jobQueue = JobQueue(
    .redis(redis),
    numWorkers: 4,
    logger: logger
)

struct MyJob: JobParameters {
    static var jobName: String { "Test" }

    let sleep: Int
}

struct MyError: Error {}
let count = Atomic<Int>(0)
let (stream, source) = AsyncStream.makeStream(of: Void.self)
jobQueue.registerJob(parameters: MyJob.self, maxRetryCount: 4) { parameters, _ in
    /*    try await Task.sleep(for: .milliseconds(parameters.sleep))
    if Int.random(in: 0..<100) < 3 {
        throw MyError()
    }
    if Int.random(in: 0..<100) > 97 {
        try await Task.sleep(for: .seconds(1))
    }*/
    let (_, value) = count.add(1, ordering: .relaxed)
    if value == 4000 { source.finish() }
}

try await withThrowingTaskGroup(of: Void.self) { group in
    let serviceGroup = ServiceGroup(
        configuration: .init(
            services: [jobQueue],
            gracefulShutdownSignals: [.sigterm, .sigint],
            logger: logger
        )
    )
    group.addTask {
        try await serviceGroup.run()
    }
    try await Task.sleep(for: .seconds(1))
    group.addTask {
        for _ in 0..<2_000 {
            try await jobQueue.push(MyJob(sleep: Int.random(in: 1..<20)))
            try await Task.sleep(for: .microseconds(Int.random(in: 1..<1000)))
        }
    }
    group.addTask {
        for _ in 0..<2_000 {
            try await jobQueue.push(MyJob(sleep: Int.random(in: 1..<20)))
            try await Task.sleep(for: .microseconds(Int.random(in: 1..<1000)))
        }
    }
    try await group.next()
    try await group.next()
    for try await _ in stream {}
    await serviceGroup.triggerGracefulShutdown()
}

let promise = redis.eventLoop.makePromise(of: Void.self)
redis.close(promise: promise)
try await promise.futureResult.get()
print("Done")
