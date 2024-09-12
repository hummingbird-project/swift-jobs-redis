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
import struct Foundation.Data
import struct Foundation.Date
import class Foundation.JSONDecoder
import struct Foundation.UUID
import Jobs
import NIOCore
import RediStack

/// Redis implementation of job queue driver
public final class RedisJobQueue: JobQueueDriver {
    public struct JobID: Sendable, CustomStringConvertible {
        let id: String
        let delayUntil: Int64

        public init(delayUntil: Date?) {
            self.id = UUID().uuidString
            self.delayUntil = Self.toMilliseconds(value: delayUntil?.timeIntervalSinceReferenceDate)
        }

        /// Initialize JobID from String
        /// - Parameter value: string value
        public init(_ value: String) {
            let parts = value.components(separatedBy: ":")
            self.id = parts[0]
            self.delayUntil = if parts.count > 1 {
                Self.toMillisecondsFromString(value: parts[1])
            } else {
                0
            }
        }

        static func toMilliseconds(value: Double?) -> Int64 {
            if let value {
                return Double(value * 1000) < Double(Int64.max) ? Int64(value * 1000) : Int64.max
            }
            return 0
        }

        static func toMillisecondsFromString(value: String) -> Int64 {
            return Int64(value) ?? 0
        }

        func isDelayed() -> Bool {
            let now = Self.toMilliseconds(value: Date.now.timeIntervalSinceReferenceDate)
            return self.delayUntil > now
        }

        var redisKey: RedisKey { .init(self.description) }

        /// String description of Identifier
        public var description: String {
            "\(self.id):\(self.delayUntil)"
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

    /// Initialize redis job queue
    /// - Parameters:
    ///   - redisConnectionPoolService: Redis connection pool
    ///   - configuration: configuration
    public init(_ redisConnectionPool: RedisConnectionPool, configuration: Configuration = .init()) {
        self.redisConnectionPool = .init(redisConnectionPool)
        self.configuration = configuration
        self.isStopped = .init(false)
    }

    /// This is run at initialization time.
    ///
    /// Will push all the jobs in the processing queue back onto to the main queue so they can
    /// be rerun
    public func onInit() async throws {
        try await self.initQueue(queueKey: self.configuration.queueKey, onInit: self.configuration.pendingJobInitialization)
        // there shouldn't be any on the processing list, but if there are we should do something with them
        try await self.initQueue(queueKey: self.configuration.processingQueueKey, onInit: self.configuration.processingJobsInitialization)
        try await self.initQueue(queueKey: self.configuration.failedQueueKey, onInit: self.configuration.failedJobsInitialization)
    }

    /// Push job data onto queue
    /// - Parameters:
    ///   - data: Job data
    /// - Returns: Queued job
    @discardableResult public func push(_ buffer: ByteBuffer, options: JobOptions) async throws -> JobID {
        let jobInstanceID = JobID(delayUntil: options.delayUntil)

        try await self.set(jobId: jobInstanceID, buffer: buffer)

        _ = try await self.redisConnectionPool.wrappedValue.lpush(jobInstanceID.redisKey, into: self.configuration.queueKey).get()
        return jobInstanceID
    }

    /// Flag job is done
    ///
    /// Removes  job id from processing queue
    /// - Parameters:
    ///   - jobId: Job id
    public func finished(jobId: JobID) async throws {
        _ = try await self.redisConnectionPool.wrappedValue.lrem(jobId.description, from: self.configuration.processingQueueKey, count: 0).get()
        try await self.delete(jobId: jobId)
    }

    /// Flag job failed to process
    ///
    /// Removes  job id from processing queue, adds to failed queue
    /// - Parameters:
    ///   - jobId: Job id
    public func failed(jobId: JobID, error: Error) async throws {
        _ = try await self.redisConnectionPool.wrappedValue.lrem(jobId.redisKey, from: self.configuration.processingQueueKey, count: 0).get()
        _ = try await self.redisConnectionPool.wrappedValue.lpush(jobId.redisKey, into: self.configuration.failedQueueKey).get()
    }

    public func stop() async {
        self.isStopped.store(true, ordering: .relaxed)
    }

    public func shutdownGracefully() async {}

    /// Get job queue metadata
    /// - Parameter key: Metadata key
    /// - Returns: Associated ByteBuffer
    public func getMetadata(_ key: String) async throws -> ByteBuffer? {
        return try await self.redisConnectionPool.wrappedValue.get(.init(key)).get().byteBuffer
    }

    /// Set job queue metadata
    /// - Parameters:
    ///   - key: Metadata key
    ///   - value: Associated ByteBuffer
    public func setMetadata(key: String, value: ByteBuffer) async throws {
        try await self.redisConnectionPool.wrappedValue.set(.init(key), to: value).get()
    }

    /// Pop Job off queue and add to pending queue
    /// - Parameter eventLoop: eventLoop to do work on
    /// - Returns: queued job
    func popFirst() async throws -> QueuedJob<JobID>? {
        let pool = self.redisConnectionPool.wrappedValue
        let key = try await pool.rpoplpush(from: self.configuration.queueKey, to: self.configuration.processingQueueKey).get()
        guard !key.isNull else {
            return nil
        }

        guard let key = String(fromRESP: key) else {
            throw RedisQueueError.unexpectedRedisKeyType
        }

        let identifier = JobID(key)

        if identifier.isDelayed() {
            _ = try await pool.lrem(identifier.redisKey, from: self.configuration.processingQueueKey, count: 0).get()
            _ = try await pool.lpush(identifier.redisKey, into: self.configuration.queueKey).get()
            return nil
        }

        if let buffer = try await self.get(jobId: identifier) {
            return .init(id: identifier, jobBuffer: buffer)
        } else {
            throw RedisQueueError.jobMissing(identifier)
        }
    }

    /// What to do with queue at initialization
    func initQueue(queueKey: RedisKey, onInit: JobInitialization) async throws {
        switch onInit {
        case .remove:
            try await self.remove(queueKey: queueKey)
        case .rerun:
            try await self.rerun(queueKey: queueKey)
        case .doNothing:
            break
        }
    }

    /// Push all the entries from list back onto the main list.
    func rerun(queueKey: RedisKey) async throws {
        while true {
            let key = try await self.redisConnectionPool.wrappedValue.rpoplpush(from: queueKey, to: self.configuration.queueKey).get()
            if key.isNull {
                return
            }
        }
    }

    /// Push all the entries from list back onto the main list.
    func remove(queueKey: RedisKey) async throws {
        while true {
            let key = try await self.redisConnectionPool.wrappedValue.rpop(from: queueKey).get()
            if key.isNull {
                break
            }
            guard let key = String(fromRESP: key) else {
                throw RedisQueueError.unexpectedRedisKeyType
            }
            let identifier = JobID(key)
            try await self.delete(jobId: identifier)
        }
    }

    func get(jobId: JobID) async throws -> ByteBuffer? {
        return try await self.redisConnectionPool.wrappedValue.get(jobId.redisKey).get().byteBuffer
    }

    func getDelayed(jobId: JobID) async throws -> ByteBuffer? {
        return try await self.redisConnectionPool.wrappedValue.get(jobId.redisKey).get().byteBuffer
    }

    func set(jobId: JobID, buffer: ByteBuffer) async throws {
        try await self.redisConnectionPool.wrappedValue.set(jobId.redisKey, to: buffer).get()
    }

    func delete(jobId: JobID) async throws {
        _ = try await self.redisConnectionPool.wrappedValue.delete(jobId.redisKey).get()
    }
}

/// extend RedisJobQueue to conform to AsyncSequence
extension RedisJobQueue {
    public typealias Element = QueuedJob<JobID>
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
        return .init(queue: self)
    }
}

extension JobQueueDriver where Self == RedisJobQueue {
    /// Return Redis driver for Job Queue
    /// - Parameters:
    ///   - redisConnectionPoolService: Redis connection pool
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
        return .bulkString(self)
    }
}
