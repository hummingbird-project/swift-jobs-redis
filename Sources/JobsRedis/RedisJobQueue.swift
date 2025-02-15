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
import RediStack

import struct Foundation.Data
import struct Foundation.Date
import class Foundation.JSONDecoder
import struct Foundation.UUID

/// Redis implementation of job queue driver
public final class RedisJobQueue: JobQueueDriver {
    public typealias JobID = String

    public enum RedisQueueError: Error, CustomStringConvertible {
        case unexpectedRedisKeyType
        case jobMissing(String)

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
    ///   - redisConnectionPool: Redis connection pool
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
        // move old pending key to new pending queue
        try await self.initQueue(queueKey: self.configuration.queueKey, onInit: .rerun)
        // should we clear the pending jobs
        if self.configuration.pendingJobInitialization == .remove {
            try await self.remove(sortedListKey: self.configuration.pendingQueueKey)
        }
        // there shouldn't be any on the processing list, but if there are we should do something with them
        try await self.initQueue(queueKey: self.configuration.processingQueueKey, onInit: self.configuration.processingJobsInitialization)
        try await self.initQueue(queueKey: self.configuration.failedQueueKey, onInit: self.configuration.failedJobsInitialization)
    }

    /// Push job data onto queue
    /// - Parameters:
    ///   - buffer: Encoded Job data
    ///   - options: Job options
    /// - Returns: Job ID
    @discardableResult public func push(_ buffer: ByteBuffer, options: JobOptions) async throws -> JobID {
        let jobInstanceID = UUID().uuidString
        try await self.addToQueue(jobInstanceID, buffer: buffer, options: options)
        return jobInstanceID
    }

    private func addToQueue(_ jobId: JobID, buffer: ByteBuffer, options: JobOptions) async throws {
        try await self.set(jobKey: jobId.redisKey, buffer: buffer)
        let date = options.delayUntil ?? .now
        _ = try await self.redisConnectionPool.wrappedValue.zadd(
            (element: jobId.redisKey, score: date.timeIntervalSince1970),
            to: self.configuration.pendingQueueKey
        ).get()
    }

    /// Flag job is done
    ///
    /// Removes  job id from processing queue
    /// - Parameters:
    ///   - jobId: Job id
    public func finished(jobId: JobID) async throws {
        _ = try await self.redisConnectionPool.wrappedValue.lrem(jobId.description, from: self.configuration.processingQueueKey, count: 0).get()
        try await self.delete(jobKey: jobId.redisKey)
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
        try await self.redisConnectionPool.wrappedValue.get(.init(key)).get().byteBuffer
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
        guard let values = try await pool.zpopmin(from: self.configuration.pendingQueueKey).get() else {
            return nil
        }
        guard let key = String(fromRESP: values.0) else {
            throw RedisQueueError.unexpectedRedisKeyType
        }
        let score = values.1
        guard Date.now.timeIntervalSince1970 > score else {
            _ = try await pool.zadd(
                (element: key, score: score),
                to: self.configuration.pendingQueueKey
            ).get()
            return nil
        }
        _ = try await pool.lpush(key, into: self.configuration.processingQueueKey).get()
        let identifier = JobID(key)

        if let buffer = try await self.get(jobKey: key.redisKey) {
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
        let nowScore = Date.now.timeIntervalSince1970
        while true {
            let key = try await self.redisConnectionPool.wrappedValue.rpop(from: queueKey).get()
            if key.isNull {
                return
            }
            _ = try await self.redisConnectionPool.wrappedValue.zadd((element: key, score: nowScore), to: self.configuration.pendingQueueKey).get()
        }
    }

    /// Push all the entries from list back onto the main list.
    func remove(queueKey: RedisKey) async throws {
        while true {
            let key = try await self.redisConnectionPool.wrappedValue.rpop(from: queueKey).get()
            if key.isNull {
                break
            }
            guard let key = RedisKey(fromRESP: key) else {
                throw RedisQueueError.unexpectedRedisKeyType
            }
            try await self.delete(jobKey: key)
        }
    }

    /// Push all the entries from list back onto the main list.
    func remove(sortedListKey: RedisKey) async throws {
        while true {
            guard let values = try await self.redisConnectionPool.wrappedValue.zpopmin(from: sortedListKey).get() else {
                break
            }
            guard let key = RedisKey(fromRESP: values.0) else {
                throw RedisQueueError.unexpectedRedisKeyType
            }
            try await self.delete(jobKey: key)
        }
    }

    func get(jobKey: RedisKey) async throws -> ByteBuffer? {
        try await self.redisConnectionPool.wrappedValue.get(jobKey).get().byteBuffer
    }

    func set(jobKey: RedisKey, buffer: ByteBuffer) async throws {
        try await self.redisConnectionPool.wrappedValue.set(jobKey, to: buffer).get()
    }

    func delete(jobKey: RedisKey) async throws {
        _ = try await self.redisConnectionPool.wrappedValue.delete(jobKey).get()
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

extension RedisJobQueue.JobID {
    var redisKey: RedisKey { .init(self) }
}

extension RedisClient {
    @inlinable
    public func zpopmin(
        from key: RedisKey
    ) -> EventLoopFuture<(RESPValue, Double)> {
        zpopmin(count: 1, from: key).flatMapThrowing { values in
            guard let first = values.first else {
                throw RedisClientError.assertionFailure(message: "Unexpected empty response")
            }
            return first
        }
    }

    @inlinable
    public func zpopmin(
        count: Int,
        from key: RedisKey
    ) -> EventLoopFuture<[(RESPValue, Double)]> {
        let args: [RESPValue] = [
            .init(from: key),
            .init(from: count)
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
