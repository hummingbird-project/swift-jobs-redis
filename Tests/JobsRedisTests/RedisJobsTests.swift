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

    func createJobQueue(
        numWorkers: Int,
        configuration: RedisJobQueue.Configuration = .init(),
        function: String = #function
    ) async throws -> JobQueue<RedisJobQueue> {
        var logger = Logger(label: "RedisJobsTests")
        logger.logLevel = .debug
        let redis = try createRedisConnectionPool(logger: logger)
        return try await JobQueue(
            .redis(redis, configuration: configuration, logger: logger),
            logger: logger,
            options: .init(
                defaultRetryStrategy: .exponentialJitter(maxBackoff: .milliseconds(10))
            )
        )
    }

    /// Helper function for test a server
    ///
    /// Creates test client, runs test function abd ensures everything is
    /// shutdown correctly
    @discardableResult public func testJobQueue<T>(
        numWorkers: Int,
        configuration: RedisJobQueue.Configuration = .init(queueName: "MyJobQueue"),
        failedJobsInitialization: RedisJobQueue.JobCleanup = .remove,
        test: (JobQueue<RedisJobQueue>) async throws -> T
    ) async throws -> T {
        let jobQueue = try await createJobQueue(numWorkers: numWorkers, configuration: configuration)
        let redisService = RedisConnectionPoolService(jobQueue.queue.redisConnectionPool.wrappedValue)
        return try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [redisService, jobQueue.processor(options: .init(numWorkers: numWorkers))],
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
            try await jobQueue.queue.cleanup(failedJobs: .remove)
            try await jobQueue.push(TestParameters())

            await self.fulfillment(of: [expectation], timeout: 5)
            try await Task.sleep(for: .milliseconds(200))

            let failedJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
                of: jobQueue.queue.configuration.failedQueueKey,
                withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
            ).get()
            XCTAssertEqual(failedJobs, 1)

            let pendingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.pendingQueueKey).get()
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

            let pendingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.pendingQueueKey).get()
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

            let pendingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.pendingQueueKey).get()
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

    /// creates job that errors on first attempt, and is left on failed queue and
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

            // stall to give job chance for job to be pushed to failed queue
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

    /// creates job that errors on first attempt, and is left on failed queue and
    /// is then removed on startup of new server
    func testRemoveAtStartup() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testRemoveAtStartup"
        }
        struct RetryError: Error {}
        let failedExpectation = XCTestExpectation(description: "TestJob failed", expectedFulfillmentCount: 1)
        let job = JobDefinition(parameters: TestParameters.self) { _, _ in
            failedExpectation.fulfill()
            throw RetryError()
        }
        try await self.testJobQueue(numWorkers: 4) { jobQueue in
            jobQueue.registerJob(job)

            try await jobQueue.push(TestParameters())

            await self.fulfillment(of: [failedExpectation], timeout: 10)

            // stall to give job chance for job to be pushed to failed queue
            try await Task.sleep(for: .milliseconds(50))
        }

        try await self.testJobQueue(numWorkers: 4, failedJobsInitialization: .remove) { jobQueue in
            jobQueue.registerJob(job)
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
            logger: logger
        )
        jobQueue.registerJob(job)
        let jobQueue2 = try await JobQueue(
            RedisJobQueue(redis, logger: logger),
            logger: logger
        )
        jobQueue2.registerJob(job)

        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [
                        redisService,
                        jobQueue.processor(options: .init(numWorkers: 2)),
                        jobQueue2.processor(options: .init(numWorkers: 2)),
                    ],
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

    func testRebuildScripts() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testBasic"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        try await self.testJobQueue(numWorkers: 1) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters)")
                _ = try await jobQueue.queue.redisConnectionPool.wrappedValue.scriptFlush(.sync).get()
                expectation.fulfill()
            }
            try await jobQueue.push(TestParameters(value: 1))

            await self.fulfillment(of: [expectation], timeout: 5)
        }
    }

    func testCancelledJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testCancelledJob"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 1)
        let jobProcessed: NIOLockedValueBox<[Int]> = .init([])
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let redis = try createRedisConnectionPool(logger: logger)
        let redisService = RedisConnectionPoolService(redis)
        let jobQueue = try await JobQueue(
            .redis(redis, configuration: .init(queueName: "MyJobQueue", pollTime: .milliseconds(50)), logger: logger),
            logger: logger
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .exponentialJitter(maxAttempts: 3, minJitter: 0.01, maxJitter: 0.25)
        ) { parameters, _ in
            jobProcessed.withLockedValue {
                $0.append(parameters.value)
            }
            expectation.fulfill()
        }
        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [redisService, jobQueue.processor()],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: Logger(label: "JobQueueService")
                )
            )
            try await jobQueue.queue.cleanup(failedJobs: .remove, processingJobs: .remove, pendingJobs: .remove)

            let cancellable = try await jobQueue.push(TestParameters(value: 30))
            try await jobQueue.push(TestParameters(value: 15))
            try await jobQueue.cancelJob(jobID: cancellable)

            group.addTask {
                try await serviceGroup.run()
            }

            await fulfillment(of: [expectation], timeout: 5)
            await serviceGroup.triggerGracefulShutdown()
        }
        XCTAssertEqual(jobProcessed.withLockedValue { $0 }, [15])
    }

    func testPausedThenResume() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testPausedAndThenResume"
            let value: Int
        }
        let counter = Counter()
        let jobRunSequence: NIOLockedValueBox<[Int]> = .init([])
        var logger = Logger(label: "JobsTests")
        logger.logLevel = .trace
        let redis = try createRedisConnectionPool(logger: logger)
        let redisService = RedisConnectionPoolService(redis)
        let jobQueue = try await JobQueue(
            .redis(redis, configuration: .init(queueName: "MyJobQueue", pollTime: .milliseconds(50)), logger: logger),
            logger: logger
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .exponentialJitter(maxAttempts: 3, minJitter: 0.01, maxJitter: 0.25)
        ) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            jobRunSequence.withLockedValue {
                $0.append(parameters.value)
            }
            counter.increment()
        }

        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [redisService, jobQueue.processor()],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: Logger(label: "JobQueueService")
                )
            )
            try await jobQueue.queue.cleanup(failedJobs: .remove, processingJobs: .remove, pendingJobs: .remove)

            let pausableJob = try await jobQueue.push(TestParameters(value: 15))
            try await jobQueue.push(TestParameters(value: 30))
            try await jobQueue.pauseJob(jobID: pausableJob)

            group.addTask {
                try await serviceGroup.run()
            }
            try await counter.waitFor(count: 1)
            try await jobQueue.resumeJob(jobID: pausableJob)
            try await counter.waitFor(count: 1)
            await serviceGroup.triggerGracefulShutdown()
        }
        XCTAssertEqual(jobRunSequence.withLockedValue { $0 }, [30, 15])
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

    func testCompletedJobRetention() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testCompletedJobRetention"
            let value: Int
        }
        let expectation = XCTestExpectation(description: "TestJob.execute was called", expectedFulfillmentCount: 3)
        try await self.testJobQueue(
            numWorkers: 1,
            configuration: .init(
                retentionPolicy: .init(completed: .retain)
            )
        ) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters.value)")
                expectation.fulfill()
            }
            try await jobQueue.push(TestParameters(value: 1))
            try await jobQueue.push(TestParameters(value: 2))
            try await jobQueue.push(TestParameters(value: 3))

            await fulfillment(of: [expectation], timeout: 10)
            try await Task.sleep(for: .milliseconds(200))

            var completedJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
                of: jobQueue.queue.configuration.completedQueueKey,
                withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
            ).get()
            XCTAssertEqual(completedJobsCount, 3)

            // Remove completed task more than 10 seconds old ie none
            try await jobQueue.queue.cleanup(completedJobs: .remove(maxAge: .seconds(10)))

            completedJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
                of: jobQueue.queue.configuration.completedQueueKey,
                withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
            ).get()
            XCTAssertEqual(completedJobsCount, 3)

            try await jobQueue.queue.cleanup(completedJobs: .remove(maxAge: .seconds(0)))

            completedJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
                of: jobQueue.queue.configuration.completedQueueKey,
                withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
            ).get()
            XCTAssertEqual(completedJobsCount, 0)
        }
    }

    func testCancelledJobRetention() async throws {
        let jobQueue = try await self.createJobQueue(
            numWorkers: 1,
            configuration: .init(retentionPolicy: .init(cancelled: .retain))
        )
        let jobName = JobName<Int>("testCancelledJobRetention")
        jobQueue.registerJob(name: jobName) { _, _ in }

        try await jobQueue.queue.cleanup(
            failedJobs: .remove,
            processingJobs: .remove,
            pendingJobs: .remove,
            cancelledJobs: .remove,
            completedJobs: .remove
        )

        for _ in 0..<150 {
            let jobId = try await jobQueue.push(jobName, parameters: 1)
            try await jobQueue.cancelJob(jobID: jobId)
        }

        var cancelledJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.cancelledQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        XCTAssertEqual(cancelledJobsCount, 150)

        try await jobQueue.queue.cleanup(cancelledJobs: .remove(maxAge: .seconds(0)))

        cancelledJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.cancelledQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        XCTAssertEqual(cancelledJobsCount, 0)
    }

    func testCancelledJobRerun() async throws {
        let jobQueue = try await self.createJobQueue(
            numWorkers: 1,
            configuration: .init(retentionPolicy: .init(cancelled: .retain))
        )
        let jobName = JobName<Int>("testCancelledJobRetention")
        jobQueue.registerJob(name: jobName) { _, _ in }

        try await jobQueue.queue.cleanup(
            failedJobs: .remove,
            processingJobs: .remove,
            pendingJobs: .remove,
            cancelledJobs: .remove,
            completedJobs: .remove
        )
        let jobId = try await jobQueue.push(jobName, parameters: 1)
        let jobId2 = try await jobQueue.push(jobName, parameters: 2)

        try await jobQueue.cancelJob(jobID: jobId)
        try await jobQueue.cancelJob(jobID: jobId2)

        var cancelledJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.cancelledQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        XCTAssertEqual(cancelledJobsCount, 2)
        var pendingJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.pendingQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        XCTAssertEqual(pendingJobsCount, 0)

        try await jobQueue.queue.cleanup(cancelledJobs: .rerun)

        cancelledJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.cancelledQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        XCTAssertEqual(cancelledJobsCount, 0)
        pendingJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.pendingQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        XCTAssertEqual(pendingJobsCount, 2)
    }

    func testCleanupProcessingJobs() async throws {
        let jobQueue = try await self.createJobQueue(
            numWorkers: 1,
            configuration: .init(retentionPolicy: .init(cancelled: .retain))
        )
        let jobName = JobName<Int>("testCancelledJobRetention")
        jobQueue.registerJob(name: jobName) { _, _ in }

        try await jobQueue.queue.cleanup(
            failedJobs: .remove,
            processingJobs: .remove,
            pendingJobs: .remove,
            cancelledJobs: .remove,
            completedJobs: .remove
        )
        let jobID = try await jobQueue.push(jobName, parameters: 1)
        let job = try await jobQueue.queue.popFirst()
        XCTAssertEqual(jobID, job?.id)
        _ = try await jobQueue.push(jobName, parameters: 1)
        _ = try await jobQueue.queue.popFirst()

        var processingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.processingQueueKey).get()
        XCTAssertEqual(processingJobs, 2)

        try await jobQueue.queue.cleanup(processingJobs: .remove)

        processingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.processingQueueKey).get()
        XCTAssertEqual(processingJobs, 0)

        let exists = try await jobQueue.queue.redisConnectionPool.wrappedValue.exists(jobID.redisKey(for: jobQueue.queue)).get()
        XCTAssertEqual(exists, 0)
    }

    func testRerunProcessingJobs() async throws {
        let jobQueue = try await self.createJobQueue(
            numWorkers: 1,
            configuration: .init(retentionPolicy: .init(cancelled: .retain))
        )
        let jobName = JobName<Int>("testCancelledJobRetention")
        jobQueue.registerJob(name: jobName) { _, _ in }

        try await jobQueue.queue.cleanup(
            failedJobs: .remove,
            processingJobs: .remove,
            pendingJobs: .remove,
            cancelledJobs: .remove,
            completedJobs: .remove
        )
        let jobID = try await jobQueue.push(jobName, parameters: 1)
        let job = try await jobQueue.queue.popFirst()
        XCTAssertEqual(jobID, job?.id)
        _ = try await jobQueue.push(jobName, parameters: 1)
        _ = try await jobQueue.queue.popFirst()

        var processingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.processingQueueKey).get()
        XCTAssertEqual(processingJobs, 2)

        try await jobQueue.queue.cleanup(processingJobs: .rerun)

        processingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.processingQueueKey).get()
        XCTAssertEqual(processingJobs, 0)
        let pendingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.pendingQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        XCTAssertEqual(pendingJobs, 2)

        let exists = try await jobQueue.queue.redisConnectionPool.wrappedValue.exists(jobID.redisKey(for: jobQueue.queue)).get()
        XCTAssertEqual(exists, 1)
    }

    func testCleanupJob() async throws {
        try await self.testJobQueue(
            numWorkers: 1,
            configuration: .init(
                retentionPolicy: .init(failed: .retain)
            )
        ) { jobQueue in
            try await self.testJobQueue(
                numWorkers: 1,
                configuration: .init(
                    queueName: "SecondQueue",
                    retentionPolicy: .init(failed: .retain)
                )
            ) { jobQueue2 in
                let (stream, cont) = AsyncStream.makeStream(of: Void.self)
                var iterator = stream.makeAsyncIterator()
                struct TempJob: Sendable & Codable {}
                let barrierJobName = JobName<TempJob>("barrier")
                jobQueue.registerJob(name: "testCleanupJob", parameters: String.self) { parameters, context in
                    throw CancellationError()
                }
                jobQueue.registerJob(name: barrierJobName, parameters: TempJob.self) { parameters, context in
                    cont.yield()
                }
                jobQueue2.registerJob(name: "testCleanupJob", parameters: String.self) { parameters, context in
                    throw CancellationError()
                }
                jobQueue2.registerJob(name: barrierJobName, parameters: TempJob.self) { parameters, context in
                    cont.yield()
                }
                try await jobQueue.push("testCleanupJob", parameters: "1")
                try await jobQueue.push("testCleanupJob", parameters: "2")
                try await jobQueue.push("testCleanupJob", parameters: "3")
                try await jobQueue.push(barrierJobName, parameters: .init())
                try await jobQueue2.push("testCleanupJob", parameters: "1")
                try await jobQueue2.push(barrierJobName, parameters: .init())

                await iterator.next()
                await iterator.next()

                var failedJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
                    of: jobQueue.queue.configuration.failedQueueKey,
                    withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
                ).get()
                XCTAssertEqual(failedJobsCount, 3)

                try await jobQueue.push(jobQueue.queue.cleanupJob, parameters: .init(failedJobs: .remove))
                try await jobQueue.push(barrierJobName, parameters: .init())

                await iterator.next()

                failedJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
                    of: jobQueue.queue.configuration.failedQueueKey,
                    withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
                ).get()
                XCTAssertEqual(failedJobsCount, 0)
                failedJobsCount = try await jobQueue2.queue.redisConnectionPool.wrappedValue.zcount(
                    of: jobQueue2.queue.configuration.failedQueueKey,
                    withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
                ).get()
                XCTAssertEqual(failedJobsCount, 1)
            }
        }
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

struct Counter {
    let stream: AsyncStream<Void>
    let continuation: AsyncStream<Void>.Continuation

    init() {
        (self.stream, self.continuation) = AsyncStream.makeStream(of: Void.self)
    }

    func increment() {
        self.continuation.yield()
    }

    func waitFor(count: Int, timeout: Duration = .seconds(5)) async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                var iterator = stream.makeAsyncIterator()
                for _ in 0..<count {
                    _ = await iterator.next()
                }

            }
            group.addTask {
                try await Task.sleep(for: timeout)
            }
            try await group.next()
            group.cancelAll()
        }
    }
}
