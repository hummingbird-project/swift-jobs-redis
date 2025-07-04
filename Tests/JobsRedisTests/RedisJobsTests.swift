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

import Foundation
import Jobs
import Logging
import NIOCore
import NIOPosix
@preconcurrency import RediStack
import ServiceLifecycle
import Synchronization
import Testing

@testable import JobsRedis

struct RedisJobsTests {
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
        var logger = Logger(label: function)
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
        configuration: RedisJobQueue.Configuration,
        failedJobsInitialization: RedisJobQueue.JobCleanup = .remove,
        test: (JobQueue<RedisJobQueue>) async throws -> T,
        function: String = #function
    ) async throws -> T {
        let jobQueue = try await createJobQueue(numWorkers: numWorkers, configuration: configuration, function: function)
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
            try await jobQueue.queue.cleanup(pendingJobs: .remove, processingJobs: .remove, failedJobs: failedJobsInitialization)
            let value = try await test(jobQueue)
            await serviceGroup.triggerGracefulShutdown()
            return value
        }
    }

    @Test func testBasic() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testBasic"
            let value: Int
        }
        let expectation = TestExpectation()
        try await self.testJobQueue(numWorkers: 1, configuration: .init(queueName: #function)) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters)")
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                expectation.trigger()
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

            try await expectation.wait(for: "TestJob.execute was called", count: 10)
        }
    }

    @Test func testMultipleWorkers() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testMultipleWorkers"
            let value: Int
        }
        let runningJobCounter = Atomic(0)
        let maxRunningJobCounter = Atomic(0)
        let expectation = TestExpectation()

        try await self.testJobQueue(numWorkers: 4, configuration: .init(queueName: #function)) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                let runningJobs = runningJobCounter.wrappingAdd(1, ordering: .relaxed).newValue
                if runningJobs > maxRunningJobCounter.load(ordering: .relaxed) {
                    maxRunningJobCounter.store(runningJobs, ordering: .relaxed)
                }
                try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
                context.logger.info("Parameters=\(parameters)")
                expectation.trigger()
                runningJobCounter.wrappingSubtract(1, ordering: .relaxed)
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

            try await expectation.wait(count: 10)

            #expect(maxRunningJobCounter.load(ordering: .relaxed) > 1)
            #expect(maxRunningJobCounter.load(ordering: .relaxed) <= 4)
        }
    }

    @Test func testDelayedJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testDelayedJob"
            let value: Int
        }
        let expectation = TestExpectation()
        let jobExecutionSequence: Mutex<[Int]> = .init([])
        try await self.testJobQueue(numWorkers: 1, configuration: .init(queueName: #function)) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, _ in
                jobExecutionSequence.withLock {
                    $0.append(parameters.value)
                }
                expectation.trigger()
            }
            try await jobQueue.push(
                TestParameters(value: 100),
                options: .init(delayUntil: Date.now.addingTimeInterval(2))
            )
            try await jobQueue.push(TestParameters(value: 50))
            try await jobQueue.push(TestParameters(value: 10))

            try await expectation.wait(for: "delayed job was called", count: 3)
        }
        jobExecutionSequence.withLock {
            #expect($0 == [50, 10, 100])
        }
    }

    @Test func testDelayedJobDoesntRun() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testDelayedJobDoesntRun"
        }
        let jobQueue = try await self.createJobQueue(
            numWorkers: 1,
            configuration: .init(queueName: #function, retentionPolicy: .init(cancelledJobs: .retain))
        )
        try await jobQueue.queue.cleanup(pendingJobs: .remove, processingJobs: .remove, failedJobs: .remove)
        try await jobQueue.push(
            TestParameters(),
            options: .init(delayUntil: Date.now.addingTimeInterval(5))
        )
        let job = try await jobQueue.queue.popFirst()
        #expect(job == nil)
        let job2 = try await jobQueue.queue.popFirst()
        #expect(job2 == nil)
    }

    @Test func testErrorRetryCount() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryCount"
        }
        let expectation = TestExpectation()
        struct FailedError: Error {}
        try await self.testJobQueue(numWorkers: 1, configuration: .init(queueName: #function)) { jobQueue in
            jobQueue.registerJob(
                parameters: TestParameters.self,
                retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .milliseconds(100))
            ) { _, _ in
                expectation.trigger()
                throw FailedError()
            }
            try await jobQueue.queue.cleanup(failedJobs: .remove)
            try await jobQueue.push(TestParameters())

            try await expectation.wait(count: 3)
            try await Task.sleep(for: .milliseconds(200))

            let failedJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
                of: jobQueue.queue.configuration.failedQueueKey,
                withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
            ).get()
            #expect(failedJobs == 1)

            let pendingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.pendingQueueKey).get()
            #expect(pendingJobs == 0)
        }
    }

    @Test func testErrorRetryAndThenSucceed() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testErrorRetryAndThenSucceed"
        }
        let expectation = TestExpectation()
        let currentJobTryCount: Mutex<Int> = .init(0)
        struct FailedError: Error {}
        try await self.testJobQueue(numWorkers: 1, configuration: .init(queueName: #function)) { jobQueue in
            jobQueue.registerJob(
                parameters: TestParameters.self,
                retryStrategy: .exponentialJitter(maxAttempts: 3, maxBackoff: .milliseconds(100))
            ) { _, _ in
                defer {
                    currentJobTryCount.withLock {
                        $0 += 1
                    }
                }
                expectation.trigger()
                if currentJobTryCount.withLock({ $0 }) == 0 {
                    throw FailedError()
                }
            }
            try await jobQueue.push(TestParameters())

            try await expectation.wait(count: 2)
            try await Task.sleep(for: .milliseconds(200))

            let failedJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.failedQueueKey).get()
            #expect(failedJobs == 0)

            let pendingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.pendingQueueKey).get()
            #expect(pendingJobs == 0)

            let processingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.processingQueueKey)
                .get()
            #expect(processingJobs == 0)
        }
        #expect(currentJobTryCount.withLock { $0 } == 2)
    }

    @Test func testJobSerialization() async throws {
        struct TestJobParameters: JobParameters {
            static let jobName = "testJobSerialization"
            let id: Int
            let message: String
        }
        let expectation = TestExpectation()
        try await self.testJobQueue(numWorkers: 1, configuration: .init(queueName: #function)) { jobQueue in
            jobQueue.registerJob(parameters: TestJobParameters.self) { parameters, _ in
                #expect(parameters.id == 23)
                #expect(parameters.message == "Hello!")
                expectation.trigger()
            }
            try await jobQueue.push(TestJobParameters(id: 23, message: "Hello!"))

            try await expectation.wait()
        }
    }

    /// Test job is cancelled on shutdown
    @Test func testShutdownJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testShutdownJob"
        }
        let expectation = TestExpectation()
        var logger = Logger(label: #function)
        logger.logLevel = .trace

        try await self.testJobQueue(numWorkers: 4, configuration: .init(queueName: #function)) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { _, _ in
                expectation.trigger()
                try await Task.sleep(for: .milliseconds(1000))
            }
            try await jobQueue.push(TestParameters())
            try await expectation.wait()

            let pendingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.pendingQueueKey).get()
            #expect(pendingJobs == 0)
            let failedJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.failedQueueKey).get()
            let processingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.processingQueueKey)
                .get()
            #expect(failedJobs + processingJobs == 1)
        }
    }

    /// test job fails to decode but queue continues to process
    @Test func testFailToDecode() async throws {
        struct TestIntParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: Int
        }
        struct TestStringParameter: JobParameters {
            static let jobName = "testFailToDecode"
            let value: String
        }
        let string: Mutex<String> = .init("")
        let expectation = TestExpectation()

        try await self.testJobQueue(numWorkers: 4, configuration: .init(queueName: #function)) { jobQueue in
            jobQueue.registerJob(parameters: TestStringParameter.self) { parameters, _ in
                string.withLock { $0 = parameters.value }
                expectation.trigger()
            }
            try await jobQueue.push(TestIntParameter(value: 2))
            try await jobQueue.push(TestStringParameter(value: "test"))
            try await expectation.wait()
        }
        string.withLock {
            #expect($0 == "test")
        }
    }

    /// creates job that errors on first attempt, and is left on failed queue and
    /// is then rerun on startup of new server
    @Test func testRerunAtStartup() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testRerunAtStartup"
        }
        struct RetryError: Error {}
        let firstTime = Atomic(true)
        let finished = Atomic(false)
        let failedExpectation = TestExpectation()
        let succeededExpectation = TestExpectation()
        let job = JobDefinition(parameters: TestParameters.self) { _, _ in
            if firstTime.compareExchange(expected: true, desired: false, ordering: .relaxed).original {
                failedExpectation.trigger()
                throw RetryError()
            }
            succeededExpectation.trigger()
            finished.store(true, ordering: .relaxed)
        }
        try await self.testJobQueue(numWorkers: 4, configuration: .init(queueName: #function)) { jobQueue in
            jobQueue.registerJob(job)

            try await jobQueue.push(TestParameters())

            try await failedExpectation.wait()

            // stall to give job chance for job to be pushed to failed queue
            try await Task.sleep(for: .milliseconds(50))
        }

        #expect(firstTime.load(ordering: .relaxed) == false)
        #expect(finished.load(ordering: .relaxed) == false)

        try await self.testJobQueue(numWorkers: 4, configuration: .init(queueName: #function), failedJobsInitialization: .rerun) { jobQueue in
            jobQueue.registerJob(job)
            try await succeededExpectation.wait()
        }

        #expect(finished.load(ordering: .relaxed) == true)
    }

    /// creates job that errors on first attempt, and is left on failed queue and
    /// is then removed on startup of new server
    @Test func testRemoveAtStartup() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testRemoveAtStartup"
        }
        struct RetryError: Error {}
        let failedExpectation = TestExpectation()
        let job = JobDefinition(parameters: TestParameters.self) { _, _ in
            failedExpectation.trigger()
            throw RetryError()
        }
        try await self.testJobQueue(numWorkers: 4, configuration: .init(queueName: #function)) { jobQueue in
            jobQueue.registerJob(job)

            try await jobQueue.push(TestParameters())

            try await failedExpectation.wait()

            // stall to give job chance for job to be pushed to failed queue
            try await Task.sleep(for: .milliseconds(50))
        }

        try await self.testJobQueue(numWorkers: 4, configuration: .init(queueName: #function), failedJobsInitialization: .remove) { jobQueue in
            jobQueue.registerJob(job)
        }
    }

    @Test func testMultipleJobQueueHandlers() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testMultipleJobQueueHandlers"
            let value: Int
        }
        let expectation = TestExpectation()
        let logger = {
            var logger = Logger(label: #function)
            logger.logLevel = .debug
            return logger
        }()
        let job = JobDefinition(parameters: TestParameters.self) { parameters, context in
            context.logger.info("Parameters=\(parameters.value)")
            try await Task.sleep(for: .milliseconds(Int.random(in: 10..<50)))
            expectation.trigger()
        }
        let redis = try createRedisConnectionPool(logger: logger)
        let redisService = RedisConnectionPoolService(redis)
        let jobQueue = try await JobQueue(
            RedisJobQueue(redis, configuration: .init(queueName: "testMultipleJobQueueHandlers"), logger: logger),
            logger: logger
        )
        jobQueue.registerJob(job)
        let jobQueue2 = try await JobQueue(
            RedisJobQueue(redis, configuration: .init(queueName: "testMultipleJobQueueHandlers2"), logger: logger),
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
                try await expectation.wait(count: 200)
                await serviceGroup.triggerGracefulShutdown()
            } catch {
                Issue.record("\(String(reflecting: error))")
                await serviceGroup.triggerGracefulShutdown()
                throw error
            }
        }
    }

    @Test func testRebuildScripts() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testBasic"
            let value: Int
        }
        let expectation = TestExpectation()
        try await self.testJobQueue(numWorkers: 1, configuration: .init(queueName: #function)) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters)")
                _ = try await jobQueue.queue.redisConnectionPool.wrappedValue.scriptFlush(.sync).get()
                expectation.trigger()
            }
            try await jobQueue.push(TestParameters(value: 1))

            try await expectation.wait()
        }
    }

    @Test func testCancelledJob() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testCancelledJob"
            let value: Int
        }
        let expectation = TestExpectation()
        let jobProcessed: Mutex<[Int]> = .init([])
        var logger = Logger(label: #function)
        logger.logLevel = .trace
        let redis = try createRedisConnectionPool(logger: logger)
        let redisService = RedisConnectionPoolService(redis)
        let jobQueue = try await JobQueue(
            .redis(redis, configuration: .init(queueName: "testCancelledJob", pollTime: .milliseconds(50)), logger: logger),
            logger: logger
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .exponentialJitter(maxAttempts: 3, minJitter: 0.01, maxJitter: 0.25)
        ) { parameters, _ in
            jobProcessed.withLock {
                $0.append(parameters.value)
            }
            expectation.trigger()
        }
        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [redisService, jobQueue.processor()],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: Logger(label: "JobQueueService")
                )
            )
            try await jobQueue.queue.cleanup(pendingJobs: .remove, processingJobs: .remove, failedJobs: .remove)

            let cancellable = try await jobQueue.push(TestParameters(value: 30))
            try await jobQueue.push(TestParameters(value: 15))
            try await jobQueue.cancelJob(jobID: cancellable)

            group.addTask {
                try await serviceGroup.run()
            }

            try await expectation.wait()
            await serviceGroup.triggerGracefulShutdown()
        }
        #expect(jobProcessed.withLock { $0 } == [15])
    }

    @Test func testPausedThenResume() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testPausedAndThenResume"
            let value: Int
        }
        let expectation = TestExpectation()
        let jobRunSequence: Mutex<[Int]> = .init([])
        var logger = Logger(label: #function)
        logger.logLevel = .trace
        let redis = try createRedisConnectionPool(logger: logger)
        let redisService = RedisConnectionPoolService(redis)
        let jobQueue = try await JobQueue(
            .redis(redis, configuration: .init(queueName: "testPausedThenResume", pollTime: .milliseconds(50)), logger: logger),
            logger: logger
        )
        jobQueue.registerJob(
            parameters: TestParameters.self,
            retryStrategy: .exponentialJitter(maxAttempts: 3, minJitter: 0.01, maxJitter: 0.25)
        ) { parameters, context in
            context.logger.info("Parameters=\(parameters)")
            jobRunSequence.withLock {
                $0.append(parameters.value)
            }
            expectation.trigger()
        }

        try await withThrowingTaskGroup(of: Void.self) { group in
            let serviceGroup = ServiceGroup(
                configuration: .init(
                    services: [redisService, jobQueue.processor()],
                    gracefulShutdownSignals: [.sigterm, .sigint],
                    logger: Logger(label: "JobQueueService")
                )
            )
            try await jobQueue.queue.cleanup(pendingJobs: .remove, processingJobs: .remove, failedJobs: .remove)

            let pausableJob = try await jobQueue.push(TestParameters(value: 15))
            try await jobQueue.push(TestParameters(value: 30))
            try await jobQueue.pauseJob(jobID: pausableJob)

            group.addTask {
                try await serviceGroup.run()
            }
            try await expectation.wait(count: 1)
            try await jobQueue.resumeJob(jobID: pausableJob)
            try await expectation.wait(count: 1)
            await serviceGroup.triggerGracefulShutdown()
        }
        #expect(jobRunSequence.withLock { $0 } == [30, 15])
    }

    @Test func testCompletedJobRetention() async throws {
        struct TestParameters: JobParameters {
            static let jobName = "testCompletedJobRetention"
            let value: Int
        }
        let expectation = TestExpectation()
        try await self.testJobQueue(
            numWorkers: 1,
            configuration: .init(
                queueName: #function,
                retentionPolicy: .init(completedJobs: .retain)
            )
        ) { jobQueue in
            jobQueue.registerJob(parameters: TestParameters.self) { parameters, context in
                context.logger.info("Parameters=\(parameters.value)")
                expectation.trigger()
            }
            try await jobQueue.push(TestParameters(value: 1))
            try await jobQueue.push(TestParameters(value: 2))
            try await jobQueue.push(TestParameters(value: 3))

            try await expectation.wait(count: 3)
            try await Task.sleep(for: .milliseconds(200))

            var completedJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
                of: jobQueue.queue.configuration.completedQueueKey,
                withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
            ).get()
            #expect(completedJobsCount == 3)

            // Remove completed task more than 10 seconds old ie none
            try await jobQueue.queue.cleanup(completedJobs: .remove(maxAge: .seconds(10)))

            completedJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
                of: jobQueue.queue.configuration.completedQueueKey,
                withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
            ).get()
            #expect(completedJobsCount == 3)

            try await jobQueue.queue.cleanup(completedJobs: .remove(maxAge: .seconds(0)))

            completedJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
                of: jobQueue.queue.configuration.completedQueueKey,
                withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
            ).get()
            #expect(completedJobsCount == 0)
        }
    }

    @Test func testCancelledJobRetention() async throws {
        let jobQueue = try await self.createJobQueue(
            numWorkers: 1,
            configuration: .init(retentionPolicy: .init(cancelledJobs: .retain))
        )
        let jobName = JobName<Int>("testCancelledJobRetention")
        jobQueue.registerJob(name: jobName) { _, _ in }

        try await jobQueue.queue.cleanup(
            pendingJobs: .remove,
            processingJobs: .remove,
            completedJobs: .remove,
            failedJobs: .remove,
            cancelledJobs: .remove
        )

        for _ in 0..<150 {
            let jobId = try await jobQueue.push(jobName, parameters: 1)
            try await jobQueue.cancelJob(jobID: jobId)
        }

        var cancelledJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.cancelledQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        #expect(cancelledJobsCount == 150)

        try await jobQueue.queue.cleanup(cancelledJobs: .remove(maxAge: .seconds(0)))

        cancelledJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.cancelledQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        #expect(cancelledJobsCount == 0)
    }

    @Test func testCancelledJobRerun() async throws {
        let jobQueue = try await self.createJobQueue(
            numWorkers: 1,
            configuration: .init(queueName: #function, retentionPolicy: .init(cancelledJobs: .retain))
        )
        let jobName = JobName<Int>("testCancelledJobRetention")
        jobQueue.registerJob(name: jobName) { _, _ in }

        try await jobQueue.queue.cleanup(
            pendingJobs: .remove,
            processingJobs: .remove,
            completedJobs: .remove,
            failedJobs: .remove,
            cancelledJobs: .remove
        )
        let jobId = try await jobQueue.push(jobName, parameters: 1)
        let jobId2 = try await jobQueue.push(jobName, parameters: 2)

        try await jobQueue.cancelJob(jobID: jobId)
        try await jobQueue.cancelJob(jobID: jobId2)

        var cancelledJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.cancelledQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        #expect(cancelledJobsCount == 2)
        var pendingJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.pendingQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        #expect(pendingJobsCount == 0)

        try await jobQueue.queue.cleanup(cancelledJobs: .rerun)

        cancelledJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.cancelledQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        #expect(cancelledJobsCount == 0)
        pendingJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.pendingQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        #expect(pendingJobsCount == 2)
    }

    @Test func testCleanupProcessingJobs() async throws {
        let jobQueue = try await self.createJobQueue(
            numWorkers: 1,
            configuration: .init(queueName: #function, retentionPolicy: .init(cancelledJobs: .retain))
        )
        let jobName = JobName<Int>("testCancelledJobRetention")
        jobQueue.registerJob(name: jobName) { _, _ in }

        try await jobQueue.queue.cleanup(
            pendingJobs: .remove,
            processingJobs: .remove,
            completedJobs: .remove,
            failedJobs: .remove,
            cancelledJobs: .remove
        )
        let jobID = try await jobQueue.push(jobName, parameters: 1)
        let job = try await jobQueue.queue.popFirst()
        #expect(jobID == job?.id)
        _ = try await jobQueue.push(jobName, parameters: 1)
        _ = try await jobQueue.queue.popFirst()

        var processingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.processingQueueKey).get()
        #expect(processingJobs == 2)

        try await jobQueue.queue.cleanup(processingJobs: .remove)

        processingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.processingQueueKey).get()
        #expect(processingJobs == 0)

        let exists = try await jobQueue.queue.redisConnectionPool.wrappedValue.exists(jobID.redisKey(for: jobQueue.queue)).get()
        #expect(exists == 0)
    }

    @Test func testRerunProcessingJobs() async throws {
        let jobQueue = try await self.createJobQueue(
            numWorkers: 1,
            configuration: .init(queueName: #function, retentionPolicy: .init(cancelledJobs: .retain))
        )
        let jobName = JobName<Int>("testCancelledJobRetention")
        jobQueue.registerJob(name: jobName) { _, _ in }

        try await jobQueue.queue.cleanup(
            pendingJobs: .remove,
            processingJobs: .remove,
            completedJobs: .remove,
            failedJobs: .remove,
            cancelledJobs: .remove
        )
        let jobID = try await jobQueue.push(jobName, parameters: 1)
        let job = try await jobQueue.queue.popFirst()
        #expect(jobID == job?.id)
        _ = try await jobQueue.push(jobName, parameters: 1)
        _ = try await jobQueue.queue.popFirst()

        var processingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.processingQueueKey).get()
        #expect(processingJobs == 2)

        try await jobQueue.queue.cleanup(processingJobs: .rerun)

        processingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.llen(of: jobQueue.queue.configuration.processingQueueKey).get()
        #expect(processingJobs == 0)
        let pendingJobs = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
            of: jobQueue.queue.configuration.pendingQueueKey,
            withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
        ).get()
        #expect(pendingJobs == 2)

        let exists = try await jobQueue.queue.redisConnectionPool.wrappedValue.exists(jobID.redisKey(for: jobQueue.queue)).get()
        #expect(exists == 1)
    }

    @Test func testCleanupJob() async throws {
        try await self.testJobQueue(
            numWorkers: 1,
            configuration: .init(
                queueName: "testCleanupJob",
                retentionPolicy: .init(failedJobs: .retain)
            )
        ) { jobQueue in
            try await self.testJobQueue(
                numWorkers: 1,
                configuration: .init(
                    queueName: "testCleanupJob2",
                    retentionPolicy: .init(failedJobs: .retain)
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
                #expect(failedJobsCount == 3)

                try await jobQueue.push(jobQueue.queue.cleanupJob, parameters: .init(failedJobs: .remove))
                try await jobQueue.push(barrierJobName, parameters: .init())

                await iterator.next()

                failedJobsCount = try await jobQueue.queue.redisConnectionPool.wrappedValue.zcount(
                    of: jobQueue.queue.configuration.failedQueueKey,
                    withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
                ).get()
                #expect(failedJobsCount == 0)
                failedJobsCount = try await jobQueue2.queue.redisConnectionPool.wrappedValue.zcount(
                    of: jobQueue2.queue.configuration.failedQueueKey,
                    withScoresBetween: (min: .inclusive(.zero), max: .inclusive(.infinity))
                ).get()
                #expect(failedJobsCount == 1)
            }
        }
    }

    @Test func testMetadata() async throws {
        let logger = Logger(label: #function)
        let redis = try createRedisConnectionPool(logger: logger)
        let jobQueue = try await RedisJobQueue(redis, logger: logger)
        let value = ByteBuffer(string: "Testing metadata")
        try await jobQueue.setMetadata(key: "test", value: value)
        let metadata = try await jobQueue.getMetadata("test")
        #expect(metadata == value)
        let value2 = ByteBuffer(string: "Testing metadata again")
        try await jobQueue.setMetadata(key: "test", value: value2)
        let metadata2 = try await jobQueue.getMetadata("test")
        #expect(metadata2 == value2)
    }

    @Test func testMultipleQueueMetadata() async throws {
        try await self.testJobQueue(numWorkers: 1, configuration: .init(queueName: "testMultipleQueueMetadata")) { jobQueue1 in
            try await self.testJobQueue(numWorkers: 1, configuration: .init(queueName: "testMultipleQueueMetadata2")) { jobQueue2 in
                try await jobQueue1.queue.setMetadata(key: "test", value: .init(string: "queue1"))
                try await jobQueue2.queue.setMetadata(key: "test", value: .init(string: "queue2"))
                let value1 = try await jobQueue1.queue.getMetadata("test")
                let value2 = try await jobQueue2.queue.getMetadata("test")
                #expect(value1.map { String(buffer: $0) } == "queue1")
                #expect(value2.map { String(buffer: $0) } == "queue2")
            }
        }
    }

    @Test func testMetadataLock() async throws {
        try await self.testJobQueue(numWorkers: 1, configuration: .init(queueName: #function)) { jobQueue in
            // 1 - acquire lock
            var result = try await jobQueue.queue.acquireLock(key: "lock", id: .init(string: "one"), expiresIn: 10)
            #expect(result == true)
            // 2 - check I can acquire lock once I already have the lock
            result = try await jobQueue.queue.acquireLock(key: "lock", id: .init(string: "one"), expiresIn: 10)
            #expect(result == true)
            // 3 - check I cannot acquire lock if a different identifer has it
            result = try await jobQueue.queue.acquireLock(key: "lock", id: .init(string: "two"), expiresIn: 10)
            #expect(result == false)
            // 4 - release lock with identifier that doesn own it
            try await jobQueue.queue.releaseLock(key: "lock", id: .init(string: "two"))
            // 5 - check I still cannot acquire lock
            result = try await jobQueue.queue.acquireLock(key: "lock", id: .init(string: "two"), expiresIn: 10)
            #expect(result == false)
            // 6 - release lock
            try await jobQueue.queue.releaseLock(key: "lock", id: .init(string: "one"))
            // 7 - check I can acquire lock after it has been released
            result = try await jobQueue.queue.acquireLock(key: "lock", id: .init(string: "two"), expiresIn: 1)
            #expect(result == true)
            // 8 - check I can acquire lock after it has expired
            try await Task.sleep(for: .seconds(1.5))
            result = try await jobQueue.queue.acquireLock(key: "lock", id: .init(string: "one"), expiresIn: 10)
            #expect(result == true)
            // 9 - release lock
            try await jobQueue.queue.releaseLock(key: "lock", id: .init(string: "one"))
        }
    }

    @Test func testMultipleQueueMetadataLock() async throws {
        try await self.testJobQueue(numWorkers: 1, configuration: .init(queueName: "queue1")) { jobQueue1 in
            try await self.testJobQueue(numWorkers: 1, configuration: .init(queueName: "queue2")) { jobQueue2 in
                let result1 = try await jobQueue1.queue.acquireLock(
                    key: "testMultipleQueueMetadataLock",
                    id: .init(string: "queue1"),
                    expiresIn: 60
                )
                let result2 = try await jobQueue2.queue.acquireLock(
                    key: "testMultipleQueueMetadataLock",
                    id: .init(string: "queue2"),
                    expiresIn: 60
                )
                #expect(result1 == true)
                #expect(result2 == true)
                try await jobQueue1.queue.releaseLock(key: "testMultipleQueueMetadataLock", id: .init(string: "queue1"))
                try await jobQueue2.queue.releaseLock(key: "testMultipleQueueMetadataLock", id: .init(string: "queue2"))
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
