//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2021-2021 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Atomics
import Foundation
import Jobs
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
@preconcurrency import RediStack
import ServiceLifecycle
import XCTest

@testable import JobsRedis

extension XCTestExpectation {
    convenience init(description: String, expectedFulfillmentCount: Int) {
        self.init(description: description)
        self.expectedFulfillmentCount = expectedFulfillmentCount
    }
}

final class RedisJobsTests: XCTestCase {
    static let redisHostname = ProcessInfo.processInfo.environment["REDIS_HOSTNAME"] ?? "localhost"

    func createRedisConnectionPool(logger: Logger) throws -> RedisConnectionPool {
        try RedisConnectionPool(
            configuration: .init(
                initialServerConnectionAddresses: [.makeAddressResolvingHost(Self.redisHostname, port: 6379)],
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
    }

    /// Helper function for test a server
    ///
    /// Creates test client, runs test function abd ensures everything is
    /// shutdown correctly
    @discardableResult public func testJobQueue<T>(
        numWorkers: Int,
        failedJobsInitialization: RedisJobQueue.JobCleanup = .remove,
        test: (JobQueue<RedisJobQueue>) async throws -> T
    ) async throws -> T {
        var logger = Logger(label: "RedisJobsTests")
        logger.logLevel = .debug
        let redis = try createRedisConnectionPool(logger: logger)
        let redisService = RedisConnectionPoolService(redis)
        let jobQueue = try await JobQueue(
            .redis(redis, configuration: .init(queueKey: "MyJobQueue", pollTime: .milliseconds(50)), logger: logger),
            numWorkers: numWorkers,
            logger: logger,
            options: .init(
                defaultRetryStrategy: .exponentialJitter(maxBackoff: .milliseconds(10))
            )
        )

        return try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [redisService, jobQueue],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: Logger(label: "JobQueueService")
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            try await jobQueue.queue.cleanup(failedJobs: failedJobsInitialization, processingJobs: .remove, pendingJobs: .remove)
            let value = try await test(jobQueue)
            await serviceGroup.triggerGracefulShutdown()
            return value
        }
    }

    func testBasic() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testBasic"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters)")
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.fulfill()
            }
            try await jobQueue.push(TestParameters(value: 1))
            try await jobQueue.push(TestParameters(value: 2))
            try await jobQueue.push(TestParameters(value: 3))
            try await jobQueue.push(TestParameters(value: 4))
            try await jobQueue.push(TestParameters(value: 5))
            try await jobQueue.push(TestParameters(value: 6))
            try await jobQueue.push(TestParameters(value: 7))
            try await jobQueue.push(TestParameters(value: 8))
            try await jobQueue.push(TestParameters(value: 9))
            try await jobQueue.push(TestParameters(value: 10))

            await self.fulfillment(of: [expectation], timeout: 5)
        }
    }

    func testMultipleWorkers() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testMultipleWorkers"
            let value: Int
        }
        let runningJobCounter = ManagedAtomic(0)
        let maxRunningJobCounter = ManagedAtomic(0)
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 10)

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                let runningJobs = runningJobCounter.wrappingIncrementThenLoad(by: 1, ordering: .relaxed)
                if runningJobs > maxRunningJobCounter.load(ordering: .relaxed) {
                    maxRunningJobCounter.store(runningJobs, ordering: .relaxed)
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                context.logger.info("Parameters=\(parameters)")
                expectation.fulfill()
                runningJobCounter.wrappingDecrement(by: 1, ordering: .relaxed)
            }

            try await jobQueue.push(TestParameters(value: 1))
            try await jobQueue.push(TestParameters(value: 2))
            try await jobQueue.push(TestParameters(value: 3))
            try await jobQueue.push(TestParameters(value: 4))
            try await jobQueue.push(TestParameters(value: 5))
            try await jobQueue.push(TestParameters(value: 6))
            try await jobQueue.push(TestParameters(value: 7))
            try await jobQueue.push(TestParameters(value: 8))
            try await jobQueue.push(TestParameters(value: 9))
            try await jobQueue.push(TestParameters(value: 10))

            await self.fulfillment(of: [expectation], timeout: 5)

            XCTAssertGreaterThan(maxRunningJobCounter.load(ordering: .relaxed), 1)
            XCTAssertLessThanOrEqual(maxRunningJobCounter.load(ordering: .relaxed), 4)
        }
    }

    func testErrorRetryCount() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryCount"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 3)
        struct FailedError: Error {}
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(
                parameters: TestParameters.self,
                retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .milliseconds(100))
            ) { _, _ in
                expectation.fulfill()
                throw FailedError()
            }
            try await jobQueue.push(TestParameters())

            await self.fulfillment(of: [expectation], timeout: 5)
            try await Task.sleep(for: .milliseconds(200))

            let failedJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.failedQueueKey).get()
            XCTAssertEqual(failedJobs, 1)

            let pendingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.queueKey).get()
            XCTAssertEqual(pendingJobs, 0)
        }
    }

    func testErrorRetryAndThenSucceed() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryAndThenSucceed"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 2)
        let currentJobTryCount: NIOLockedValueBox<Int> = .init(0)
        struct FailedError: Error {}
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(
                parameters: TestParameters.self,
                retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .milliseconds(100))
            ) { _, _ in
                defer {
                    currentJobTryCount.withLockedValue {
                        $0 += 1
                    }
                }
                expectation.fulfill()
                if currentJobTryCount.withLockedValue({ $0 }) == 0 {
                    throw FailedError()
                }
            }
            try await jobQueue.push(TestParameters())

            await self.fulfillment(of: [expectation], timeout: 5)
            try await Task.sleep(for: .milliseconds(200))

            let failedJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.failedQueueKey).get()
            XCTAssertEqual(failedJobs, 0)

            let pendingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.queueKey).get()
            XCTAssertEqual(pendingJobs, 0)

            let processingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.processingQueueKey)
                .get()
            XCTAssertEqual(processingJobs, 0)
        }
        XCTAssertEqual(currentJobTryCount.withLockedValue { $0 }, 2)
    }

    func testJobSerialization() async throws {
        struct TestJobParameters: JobParameters {
            static let jobName = "testJobSerialization"
            let id: Int
            let message: String
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called")
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(parameters: TestJobParameters.self) { parameters, _ in
                XCTAssertEqual(parameters.id, 23)
                XCTAssertEqual(parameters.message, "Hello!")
                expectation.fulfill()
            }
            try await jobQueue.push(TestJobParameters(id: 23, message: "Hello!"))

            await self.fulfillment(of: [expectation], timeout: 5)
        }
    }

    /*func testJobId() async throws {
        let job = RedisJobQueue.PendingJobID(jobID: .init(), delayUntil: nil)
        XCTAssertEqual(job.delayUntil, 0)
        XCTAssertEqual(job.isDelayed(), false)
        let futureDate = Date().addingTimeInterval(100)
        let delayedJob = RedisJobQueue.PendingJobID(jobID: .init(), delayUntil: futureDate)
        XCTAssertEqual(delayedJob.isDelayed(), true)
        let respValue = delayedJob.convertedToRESPValue()
        let delayedJob2 = RedisJobQueue.PendingJobID(fromRESP: respValue)
        XCTAssertEqual(delayedJob, delayedJob2)
    }*/

    func testDelayedJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testDelayedJob"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 3)
        let jobExecutionSequence: NIOLockedValueBox<[Int]> = .init([])
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, _ in
                jobExecutionSequence.withLockedValue {
                    $0.append(parameters.value)
                }
                expectation.fulfill()
            }
            try await jobQueue.push(
                TestParameters(value: 100),
                options: .init(delayUntil: Date.now.addingTimeInterval(1))
            )
            try await jobQueue.push(TestParameters(value: 50))
            try await jobQueue.push(TestParameters(value: 10))

            await self.fulfillment(of: [expectation], timeout: 5)
        }
        jobExecutionSequence.withLockedValue {
            XCTAssertEqual($0, [50, 10, 100])
        }
    }

    /// Test job is cancelled on shutdown
    func testShutdownJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testShutdownJob"
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { _, _ in
                expectation.fulfill()
                try await Task.sleep(for: .milliseconds(1000))
            }
            try await jobQueue.push(TestParameters())
            await self.fulfillment(of: [expectation], timeout: 5)

            let pendingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.queueKey).get()
            XCTAssertEqual(pendingJobs, 0)
            let failedJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.failedQueueKey).get()
            let processingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.processingQueueKey)
                .get()
            XCTAssertEqual(failedJobs + processingJobs, 1)
        }
    }

    /// test job fails to decode but queue continues to process
    func testFailToDecode() async throws {
        struct TestIntParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: Int
        }
        struct TestStringParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: String
        }
        let string: NIOLockedValueBox<String> = .init("")
        let expectation = XCTestExpectation(description: "job was called", expectedFulfillmentCount: 1)

        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(parameters: TestStringParameter.self) { parameters, _ in
                string.withLockedValue { $0 = parameters.value }
                expectation.fulfill()
            }
            try await jobQueue.push(TestIntParameter(value: 2))
            try await jobQueue.push(TestStringParameter(value: "test"))
            await self.fulfillment(of: [expectation], timeout: 5)
        }
        string.withLockedValue {
            XCTAssertEqual($0, "test")
        }
    }

    /// creates job that errors on first attempt, and is left on processing queue and
    /// is then rerun on startup of new server
    func testRerunAtStartup() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testRerunAtStartup"
        }
        struct RetryError: Error {}
        let firstTime = ManagedAtomic(true)
        let finished = ManagedAtomic(false)
        let failedExpectation = XCTestExpectation(description: "TestJob failed", expectedFulfillmentCount: 1)
        let succeededExpectation = XCTestExpectation(description: "TestJob2 succeeded", expectedFulfillmentCount: 1)
        let job = JobDefinition(parameters: TestParameters.self) { _, _ in
            if firstTime.compareExchange(expected: true, desired: false, ordering: .relaxed).original {
                failedExpectation.fulfill()
                throw RetryError()
            }
            succeededExpectation.fulfill()
            finished.store(true, ordering: .relaxed)
        }
        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(job)

            try await jobQueue.push(TestParameters())

            await self.fulfillment(of: [failedExpectation], timeout: 10)

            // stall to give job chance to start running
            try await Task.sleep(for: .milliseconds(50))

            XCTAssertFalse(firstTime.load(ordering: .relaxed))
            XCTAssertFalse(finished.load(ordering: .relaxed))
        }

        try await self.testJobQueue(numWorkers: 4, failedJobsInitialization: .rerun) { jobQueue in
            jobQueue.registerJob(job)
            await self.fulfillment(of: [succeededExpectation], timeout: 10)
            XCTAssertTrue(finished.load(ordering: .relaxed))
        }
    }

    func testMultipleJobQueueHandlers() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testMultipleJobQueueHandlers"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 200)
        let logger = {
            var logger = Logger(label: "JobsTests")
            logger.logLevel = .debug
            return logger
        }()
        let job = JobDefinition(parameters: TestParameters.self) { parameters, context in
            context.logger.info("Parameters=\(parameters.value)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.fulfill()
        }
        let redis = try createRedisConnectionPool(logger: logger)
        let redisService = RedisConnectionPoolService(redis)
        let jobQueue = try await JobQueue(
            RedisJobQueue(redis, logger: logger),
            numWorkers: 2,
            logger: logger
        )
        jobQueue.registerJob(job)
        let jobQueue2 = try await JobQueue(
            RedisJobQueue(redis, logger: logger),
            numWorkers: 2,
            logger: logger
        )
        jobQueue2.registerJob(job)

        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [redisService, jobQueue, jobQueue2],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: logger
                )
            )
            group.addTask {
                try await serviceGroup.run()
            }
            do {
                for i in 0..<200 {
                    try await jobQueue.push(TestParameters(value: i))
                }
                await self.fulfillment(of: [expectation], timeout: 5)
                await serviceGroup.triggerGracefulShutdown()
            } catch {
                XCTFail("\(String(reflecting: error))")
                await serviceGroup.triggerGracefulShutdown()
                throw error
            }
        }
    }

    func testMetadata() async throws {
        let logger = Logger(label: "Jobs")
        let redis = try createRedisConnectionPool(logger: logger)
        let jobQueue = try await RedisJobQueue(redis, logger: logger)
        let value = ByteBuffer(string: "Testing metadata")
        try await jobQueue.setMetadata(key: "test", value: value)
        let metadata = try await jobQueue.getMetadata("test")
        XCTAssertEqual(metadata, value)
        let value2 = ByteBuffer(string: "Testing metadata again")
        try await jobQueue.setMetadata(key: "test", value: value2)
        let metadata2 = try await jobQueue.getMetadata("test")
        XCTAssertEqual(metadata2, value2)
    }
}

struct RedisConnectionPoolService: Service {
    let pool: RedisConnectionPool

    init(_ pool: RedisConnectionPool) {
        self.pool = pool
    }

    public func run() async throws {
        // Wait for graceful shutdown and ignore cancellation error
        try? await gracefulShutdown()
        // close connection pool
        let promise = self.pool.eventLoop.makePromise(of: Void.self)
        self.pool.close(promise: promise)
        return try await promise.futureResult.get()
    }
}
