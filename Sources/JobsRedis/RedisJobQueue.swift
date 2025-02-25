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
import NIOCore
@preconcurrency import RediStack

import struct Foundation.Data
import struct Foundation.Date
import class Foundation.JSONDecoder
import struct Foundation.UUID

/// Redis implementation of job queue driver
public final class RedisJobQueue: JobQueueDriver {
    public struct JobID: Sendable, CustomStringConvertible, Equatable {
        let value: String

        init() {
            self.value = UUID().uuidString
        }

        init(value: String) {
            self.value = value
        }

        var redisKey: RedisKey { .init(self.description) }

        /// String description of Identifier
        public var description: String {
            self.value
        }
    }

    public struct PendingJobID: RESPValueConvertible, Equatable {
        let jobID: JobID
        let delayUntil: Int64

        public init(jobID: JobID, delayUntil: Date?) {
            self.jobID = .init()
            self.delayUntil = Self.toMilliseconds(value: delayUntil?.timeIntervalSince1970)
        }

        public init?(fromRESP value: RediStack.RESPValue) {
            guard let string = String(fromRESP: value) else { return nil }
            let parts = string.components(separatedBy: ":")
            self.jobID = .init(value: parts[0])
            self.delayUntil =
                if parts.count > 1 {
                    Self.toMillisecondsFromString(value: parts[1])
                } else {
                    0
                }
        }

        public func convertedToRESPValue() -> RediStack.RESPValue {
            "\(self.jobID):\(self.delayUntil)".convertedToRESPValue()
        }

        static func toMilliseconds(value: Double?) -> Int64 {
            if let value {
                return Double(value * 1000) < Double(Int64.max) ? Int64(value * 1000) : Int64.max
            }
            return 0
        }

        static func toMillisecondsFromString(value: String) -> Int64 {
            Int64(value) ?? 0
        }

        func isDelayed() -> Bool {
            let now = Self.toMilliseconds(value: Date.now.timeIntervalSince1970)
            return self.delayUntil > now
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
    public init(_ redisConnectionPool: RedisConnectionPool, configuration: Configuration = .init()) {
        self.redisConnectionPool = .init(redisConnectionPool)
        self.configuration = configuration
        self.isStopped = .init(false)
        self.jobRegistry = .init()
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
    ///   - options: JobOptions
    public func retry<Parameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobOptions) async throws {
        try await self.finished(jobID: id)
        try await self.push(jobID: id, jobRequest: jobRequest, options: options)
    }

    /// Helper for enqueuing jobs
    private func push<Parameters>(jobID: JobID, jobRequest: JobRequest<Parameters>, options: JobOptions) async throws {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        let pendingJobID = PendingJobID(jobID: jobID, delayUntil: options.delayUntil)
        try await self.addToQueue(pendingJobID, buffer: buffer)
    }

    private func addToQueue(_ pendingJobID: PendingJobID, buffer: ByteBuffer) async throws {
        try await self.set(jobID: pendingJobID.jobID, buffer: buffer)
        _ = try await self.redisConnectionPool.wrappedValue.lpush(pendingJobID, into: self.configuration.queueKey).get()
    }

    /// Flag job is done
    ///
    /// Removes  job id from processing queue
    /// - Parameters:
    ///   - jobID: Job id
    public func finished(jobID: JobID) async throws {
        _ = try await self.redisConnectionPool.wrappedValue.lrem(jobID.description, from: self.configuration.processingQueueKey, count: 0).get()
        try await self.delete(jobID: jobID)
    }

    /// Flag job failed to process
    ///
    /// Removes  job id from processing queue, adds to failed queue
    /// - Parameters:
    ///   - jobID: Job id
    public func failed(jobID: JobID, error: Error) async throws {
        _ = try await self.redisConnectionPool.wrappedValue.lrem(jobID.redisKey, from: self.configuration.processingQueueKey, count: 0).get()
        _ = try await self.redisConnectionPool.wrappedValue.lpush(jobID.redisKey, into: self.configuration.failedQueueKey).get()
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
        let pool = self.redisConnectionPool.wrappedValue
        let pendingJobKey = try await pool.rpop(from: self.configuration.queueKey).get()
        guard !pendingJobKey.isNull else {
            return nil
        }

        guard let pendingJobID = PendingJobID(fromRESP: pendingJobKey) else {
            throw RedisQueueError.unexpectedRedisKeyType
        }

        guard !pendingJobID.isDelayed() else {
            _ = try await pool.lpush(pendingJobID, into: self.configuration.queueKey).get()
            return nil
        }
        let jobID = pendingJobID.jobID

        _ = try await pool.lpush(jobID.redisKey, into: self.configuration.processingQueueKey).get()

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
                let key = try await self.redisConnectionPool.wrappedValue.rpop(from: queueKey).get()
                if key.isNull {
                    break
                }
                guard let key = PendingJobID(fromRESP: key) else {
                    throw RedisQueueError.unexpectedRedisKeyType
                }
                try await self.delete(jobID: key.jobID)
            }
        default:
            break
        }
    }

    /// Push all the entries from list back onto the main list.
    func rerunQueue(queueKey: RedisKey) async throws {
        while true {
            let key = try await self.redisConnectionPool.wrappedValue.rpoplpush(from: queueKey, to: self.configuration.queueKey).get()
            if key.isNull {
                return
            }
        }
    }

    /// Push all the entries from list back onto the main list.
    func removeQueue(queueKey: RedisKey) async throws {
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

    func set(jobID: JobID, buffer: ByteBuffer) async throws {
        try await self.redisConnectionPool.wrappedValue.set(jobID.redisKey, to: buffer).get()
    }

    func delete(jobID: JobID) async throws {
        _ = try await self.redisConnectionPool.wrappedValue.delete(jobID.redisKey).get()
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

extension JobQueueDriver where Self == RedisJobQueue {
    /// Return Redis driver for Job Queue
    /// - Parameters:
    ///   - redisConnectionPool: Redis connection pool
    ///   - configuration: configuration
    public static func redis(_ redisConnectionPool: RedisConnectionPool, configuration: RedisJobQueue.Configuration = .init()) -> Self {
        .init(redisConnectionPool, configuration: configuration)
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
