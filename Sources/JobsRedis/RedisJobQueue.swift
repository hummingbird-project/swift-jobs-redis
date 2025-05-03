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
import Jobs
import Logging
import NIOCore
@preconcurrency import RediStack

import struct Foundation.Data
import struct Foundation.Date
import class Foundation.JSONDecoder
import struct Foundation.UUID

/// Redis implementation of job queue driver
public final class RedisJobQueue: JobQueueDriver {
    public struct JobID: Sendable, CustomStringConvertible, Equatable, RESPValueConvertible {
        let value: String

        init() {
            self.value = UUID().uuidString
        }

        init(value: String) {
            self.value = value
        }

        public init?(fromRESP value: RediStack.RESPValue) {
            guard let string = String(fromRESP: value) else { return nil }
            self.value = string
        }

        public func convertedToRESPValue() -> RediStack.RESPValue {
            self.value.convertedToRESPValue()
        }

        var redisKey: RedisKey { .init(self.description) }

        /// String description of Identifier
        public var description: String {
            self.value
        }
    }

    /// Options for job pushed to queue
    public struct JobOptions: JobOptionsProtocol {
        /// Delay running job until
        public var delayUntil: Date?

        /// Default initializer for JobOptions
        public init() {
            self.delayUntil = nil
        }

        ///  Initializer for JobOptions
        /// - Parameter delayUntil: Whether job execution should be delayed until a later date
        public init(delayUntil: Date?) {
            self.delayUntil = delayUntil
        }
    }

    public enum RedisQueueError: Error, CustomStringConvertible {
        case unexpectedRedisKeyType
        case jobMissing(JobID)

        public var description: String {
            switch self {
            case .unexpectedRedisKeyType:
                return "Unexpected redis key type"
            case .jobMissing(let value):
                return "Job associated with \(value) is missing"
            }
        }
    }

    let redisConnectionPool: UnsafeTransfer<RedisConnectionPool>
    let configuration: Configuration
    let isStopped: ManagedAtomic<Bool>
    let scripts: RedisScripts
    let didElectAsLeader: ManagedAtomic<Bool>

    /// what to do with failed/processing jobs from last time queue was handled
    public enum JobCleanup: Sendable {
        case doNothing
        case rerun
        case remove
    }

    /// Initialize redis job queue
    /// - Parameters:
    ///   - redisConnectionPool: Redis connection pool
    ///   - configuration: configuration
    ///   - logger: Logger used by RedisJobQueue
    public init(_ redisConnectionPool: RedisConnectionPool, configuration: Configuration = .init(), logger: Logger) async throws {
        self.redisConnectionPool = .init(redisConnectionPool)
        self.configuration = configuration
        self.isStopped = .init(false)
        self.jobRegistry = .init()
        self.scripts = try await Self.uploadScripts(redisConnectionPool: redisConnectionPool, logger: logger)
        self.didElectAsLeader = .init(false)
    }

    ///  Cleanup job queues
    ///
    /// This function is used to re-run or delete jobs in a certain state. Failed jobs can be
    /// pushed back into the pending queue to be re-run or removed. When called at startup in
    /// theory no job should be set to processing, or set to pending but not in the queue. but if
    /// your job server crashes these states are possible, so we also provide options to re-queue
    /// these jobs so they are run again.
    ///
    /// The job queue needs to be running when you call cleanup. You can call `cleanup` with
    /// `failedJobs`` set to whatever you like at any point to re-queue failed jobs. Moving processing
    /// or pending jobs should only be done if you are certain there is nothing else processing
    /// the job queue.
    ///
    /// - Parameters:
    ///   - failedJobs: What to do with jobs tagged as failed
    ///   - processingJobs: What to do with jobs tagged as processing
    ///   - pendingJobs: What to do with jobs tagged as pending
    /// - Throws:
    public func cleanup(
        failedJobs: JobCleanup = .doNothing,
        processingJobs: JobCleanup = .doNothing,
        pendingJobs: JobCleanup = .doNothing
    ) async throws {
        try await self.initPendingQueue(queueKey: self.configuration.queueKey, onInit: pendingJobs)
        // there shouldn't be any on the processing list, but if there are we should do something with them
        try await self.initQueue(queueKey: self.configuration.processingQueueKey, onInit: processingJobs)
        try await self.initQueue(queueKey: self.configuration.failedQueueKey, onInit: failedJobs)
    }

    ///  Register job
    /// - Parameters:
    ///   - job: Job Definition
    public func registerJob<Parameters: JobParameters>(_ job: JobDefinition<Parameters>) {
        self.jobRegistry.registerJob(job)
    }

    /// Push job data onto queue
    /// - Parameters:
    ///   - jobRequest: Job request
    ///   - options: Job options
    /// - Returns: Job ID
    @discardableResult public func push<Parameters: JobParameters>(_ jobRequest: JobRequest<Parameters>, options: JobOptions) async throws -> JobID {
        let jobInstanceID = JobID()
        try await self.push(jobID: jobInstanceID, jobRequest: jobRequest, options: options)
        return jobInstanceID
    }

    /// Retry job data onto queue
    /// - Parameters:
    ///   - id: Job instance ID
    ///   - jobRequest: Job request
    ///   - options: Job retry options
    public func retry<Parameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobRetryOptions) async throws {
        let options = JobOptions(delayUntil: options.delayUntil)
        try await self.finished(jobID: id)
        try await self.push(jobID: id, jobRequest: jobRequest, options: options)
    }

    /// Helper for enqueuing jobs
    private func push<Parameters>(jobID: JobID, jobRequest: JobRequest<Parameters>, options: JobOptions) async throws {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        _ = try await self.scripts.addToQueue.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [jobID.redisKey, self.configuration.queueKey],
            arguments: [
                .init(from: buffer),
                .init(from: jobID.redisKey),
                .init(from: options.delayUntil?.timeIntervalSince1970 ?? Date.now.timeIntervalSince1970),
            ]
        )
    }

    /// Flag job is done
    ///
    /// Removes  job id from processing queue
    /// - Parameters:
    ///   - jobID: Job id
    public func finished(jobID: JobID) async throws {
        _ = try await self.scripts.delete.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [self.configuration.processingQueueKey, jobID.redisKey],
            arguments: [.init(from: jobID.redisKey)]
        )
    }

    /// Flag job failed to process
    ///
    /// Removes  job id from processing queue, adds to failed queue
    /// - Parameters:
    ///   - jobID: Job id
    public func failed(jobID: JobID, error: Error) async throws {
        _ = try await self.scripts.moveToFailed.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [self.configuration.processingQueueKey, self.configuration.failedQueueKey],
            arguments: [.init(from: jobID.redisKey)]
        )
    }

    public func stop() async {
        self.isStopped.store(true, ordering: .relaxed)
    }

    public func shutdownGracefully() async {}

    /// Get job queue metadata
    /// - Parameter key: Metadata key
    /// - Returns: Associated ByteBuffer
    public func getMetadata(_ key: String) async throws -> ByteBuffer? {
        let key = "\(self.configuration.metadataKeyPrefix).\(key)"
        return try await self.redisConnectionPool.wrappedValue.get(.init(key)).get().byteBuffer
    }

    /// Set job queue metadata
    /// - Parameters:
    ///   - key: Metadata key
    ///   - value: Associated ByteBuffer
    public func setMetadata(key: String, value: ByteBuffer) async throws {
        let key = "\(self.configuration.metadataKeyPrefix).\(key)"
        try await self.redisConnectionPool.wrappedValue.set(.init(key), to: value).get()
    }

    /// Pop Job off queue and add to pending queue
    /// - Parameter eventLoop: eventLoop to do work on
    /// - Returns: queued job
    func popFirst() async throws -> JobQueueResult<JobID>? {
        let value = try await self.scripts.pop.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [self.configuration.queueKey, self.configuration.processingQueueKey],
            arguments: [.init(from: Date.now.timeIntervalSince1970)]
        )
        guard let jobID = JobID(fromRESP: value) else {
            return nil
        }

        if let buffer = try await self.get(jobID: jobID) {
            do {
                let jobInstance = try self.jobRegistry.decode(buffer)
                return .init(id: jobID, result: .success(jobInstance))
            } catch let error as JobQueueError {
                return .init(id: jobID, result: .failure(error))
            }
        } else {
            return .init(id: jobID, result: .failure(JobQueueError(code: .unrecognisedJobId, jobName: nil)))
        }
    }

    /// What to do with queue at initialization
    func initQueue(queueKey: RedisKey, onInit: JobCleanup) async throws {
        switch onInit {
        case .remove:
            try await self.removeQueue(queueKey: queueKey)
        case .rerun:
            try await self.rerunQueue(queueKey: queueKey)
        case .doNothing:
            break
        }
    }

    /// What to do with queue at initialization
    func initPendingQueue(queueKey: RedisKey, onInit: JobCleanup) async throws {
        switch onInit {
        case .remove:
            while true {
                let values = try await self.redisConnectionPool.wrappedValue._zpopmin(count: 1, from: self.configuration.queueKey).get()
                guard let value = values.first else {
                    break
                }
                guard let jobID = JobID(fromRESP: value.0) else {
                    throw RedisQueueError.unexpectedRedisKeyType
                }
                try await self.delete(jobID: jobID)
            }
        default:
            break
        }
    }

    /// Push all the entries from list back onto the main list.
    func rerunQueue(queueKey: RedisKey) async throws {
        _ = try await self.scripts.rerunQueue.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [queueKey, self.configuration.queueKey],
            arguments: []
        )
    }

    /// Delete all entries from queue
    func removeQueue(queueKey: RedisKey) async throws {
        // Cannot use a script for this as it edits keys that are not input keys
        while true {
            let key = try await self.redisConnectionPool.wrappedValue.rpop(from: queueKey).get()
            if key.isNull {
                break
            }
            guard let key = String(fromRESP: key) else {
                throw RedisQueueError.unexpectedRedisKeyType
            }
            let identifier = JobID(value: key)
            try await self.delete(jobID: identifier)
        }
    }

    func get(jobID: JobID) async throws -> ByteBuffer? {
        try await self.redisConnectionPool.wrappedValue.get(jobID.redisKey).get().byteBuffer
    }

    func delete(jobID: JobID) async throws {
        _ = try await self.redisConnectionPool.wrappedValue.delete(jobID.redisKey).get()
    }

    func tryToAcquireLock() async throws {
        _ = try await self.redisConnectionPool.wrappedValue.set(
            self.configuration.lockKeyPrefix,
            to: self.configuration.lockValue,
            onCondition: .keyDoesNotExist,
            expiration: self.configuration.lockKeyDuration
        ).get()

        let result = try await self.redisConnectionPool.wrappedValue.get(self.configuration.lockKeyPrefix).get()

        var isLeader: Bool = false
        // Should never happen since the value would have been set ealier
        if result.isNull {
            isLeader = false
        }

        if String(fromRESP: result) == self.configuration.lockValue {
            isLeader = true
        }
        didElectAsLeader.store(isLeader, ordering: .relaxed)
    }

    public func isLeader() async -> Bool {
        return didElectAsLeader.load(ordering: .relaxed)
    }

    let jobRegistry: JobRegistry
}

/// extend RedisJobQueue to conform to AsyncSequence
extension RedisJobQueue {
    public typealias Element = JobQueueResult<JobID>
    public struct AsyncIterator: AsyncIteratorProtocol {
        let queue: RedisJobQueue

        public func next() async throws -> Element? {
            while true {
                if self.queue.isStopped.load(ordering: .relaxed) {
                    return nil
                }

                try await queue.tryToAcquireLock()

                if let job = try await queue.popFirst() {
                    return job
                }
                // we only sleep if we didn't receive a job
                try await Task.sleep(for: self.queue.configuration.pollTime)
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        .init(queue: self)
    }
}

extension RedisJobQueue: CancellableJobQueue {
    /// Cancels a job
    ///
    /// Removes it from the pending queue
    /// - Parameters:
    ///  - jobID: Job id
    public func cancel(jobID: JobID) async throws {
        _ = try await self.scripts.cancel.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [self.configuration.queueKey, jobID.redisKey],
            arguments: [.init(from: jobID.redisKey)]
        )
    }
}

extension RedisJobQueue: ResumableJobQueue {
    /// Temporarily remove job from pending queue
    ///
    /// Removes it from the pending queue, adds to paused queue
    /// - Parameters:
    ///  - jobID: Job id
    public func pause(jobID: JobID) async throws {
        _ = try await self.scripts.pauseResume.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [self.configuration.queueKey, self.configuration.pausedQueueKey],
            arguments: [.init(from: jobID.redisKey)]
        )
    }

    /// Moved paused job back onto pending queue
    ///
    /// Removes it from the paused queue, adds to pending queue
    /// - Parameters:
    ///  - jobID: Job id
    public func resume(jobID: JobID) async throws {
        _ = try await self.scripts.pauseResume.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [self.configuration.pausedQueueKey, self.configuration.queueKey],
            arguments: [.init(from: jobID.redisKey)]
        )
    }
}

extension JobQueueDriver where Self == RedisJobQueue {
    /// Return Redis driver for Job Queue
    /// - Parameters:
    ///   - redisConnectionPool: Redis connection pool
    ///   - configuration: configuration
    ///   - logger: Logger used by RedisJobQueue
    public static func redis(
        _ redisConnectionPool: RedisConnectionPool,
        configuration: RedisJobQueue.Configuration = .init(),
        logger: Logger
    ) async throws -> Self {
        try await .init(redisConnectionPool, configuration: configuration, logger: logger)
    }
}

// Extend ByteBuffer so that is conforms to `RESPValueConvertible`. Really not sure why
// this isnt available already
#if compiler(>=6.0)
extension ByteBuffer: @retroactive RESPValueConvertible {}
#else
extension ByteBuffer: RESPValueConvertible {}
#endif
extension ByteBuffer {
    public init?(fromRESP value: RESPValue) {
        guard let buffer = value.byteBuffer else { return nil }
        self = buffer
    }

    public func convertedToRESPValue() -> RESPValue {
        .bulkString(self)
    }
}

extension RedisClient {
    /// The version of zpopmin in RediStack does not work, so until a fix is merged I have
    /// implemented a version of it here
    @inlinable
    public func _zpopmin(
        count: Int,
        from key: RedisKey
    ) -> EventLoopFuture<[(RESPValue, Double)]> {
        let args: [RESPValue] = [
            .init(from: key),
            .init(from: count),
        ]
        return self.send(command: "ZPOPMIN", with: args).flatMapThrowing { value in
            guard let values = [RESPValue](fromRESP: value) else { throw RedisClientError.failedRESPConversion(to: [RESPValue].self) }
            var index = 0
            var result: [(RESPValue, Double)] = .init()
            while index < values.count - 1 {
                guard let score = Double(fromRESP: values[index + 1]) else {
                    throw RedisClientError.assertionFailure(message: "Unexpected response: '\(values[index + 1])'")
                }
                let value = values[index]
                result.append((value, score))
                index += 2
            }
            return result
        }
    }
}
