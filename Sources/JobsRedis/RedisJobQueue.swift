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

import Jobs
import Logging
import NIOCore
@preconcurrency import RediStack
import Synchronization

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Redis implementation of job queue driver
public final class RedisJobQueue: JobQueueDriver {
    public struct JobID: Sendable, CustomStringConvertible, Equatable, RESPValueConvertible {
        let value: String

        @usableFromInline
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

        @usableFromInline
        func redisKey(for queue: RedisJobQueue) -> RedisKey { .init("\(queue.configuration.queueName)/\(self.description)") }

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

    @usableFromInline
    let redisConnectionPool: UnsafeTransfer<RedisConnectionPool>
    @usableFromInline
    let configuration: Configuration
    @usableFromInline
    let isStopped: Atomic<Bool>
    @usableFromInline
    let scripts: RedisScripts

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
        self.registerCleanupJob()
    }

    ///  Register job
    /// - Parameters:
    ///   - job: Job Definition
    public func registerJob<Parameters>(_ job: JobDefinition<Parameters>) {
        self.jobRegistry.registerJob(job)
    }

    /// Push job data onto queue
    /// - Parameters:
    ///   - jobRequest: Job request
    ///   - options: Job options
    /// - Returns: Job ID
    @discardableResult
    @inlinable
    public func push<Parameters>(_ jobRequest: JobRequest<Parameters>, options: JobOptions) async throws -> JobID {
        let jobInstanceID = JobID()
        try await self.push(jobID: jobInstanceID, jobRequest: jobRequest, options: options)
        return jobInstanceID
    }

    /// Retry job data onto queue
    /// - Parameters:
    ///   - id: Job instance ID
    ///   - jobRequest: Job request
    ///   - options: Job retry options
    @inlinable
    public func retry<Parameters>(_ id: JobID, jobRequest: JobRequest<Parameters>, options: JobRetryOptions) async throws {
        let options = JobOptions(delayUntil: options.delayUntil)
        try await self.finished(jobID: id)
        try await self.push(jobID: id, jobRequest: jobRequest, options: options)
    }

    /// Helper for enqueuing jobs
    @usableFromInline
    func push<Parameters>(jobID: JobID, jobRequest: JobRequest<Parameters>, options: JobOptions) async throws {
        let buffer = try self.jobRegistry.encode(jobRequest: jobRequest)
        _ = try await self.scripts.addToQueue.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [jobID.redisKey(for: self), self.configuration.pendingQueueKey],
            arguments: [
                .init(from: buffer),
                .init(from: jobID.description),
                .init(from: options.delayUntil?.timeIntervalSince1970 ?? Date.now.timeIntervalSince1970),
            ]
        )
    }

    /// Flag job is done
    ///
    /// Removes  job id from processing queue
    /// - Parameters:
    ///   - jobID: Job id
    @inlinable
    public func finished(jobID: JobID) async throws {
        if self.configuration.retentionPolicy.completedJobs == .retain {
            _ = try await self.scripts.completedAndRetain.runScript(
                on: self.redisConnectionPool.wrappedValue,
                keys: [self.configuration.processingQueueKey, self.configuration.completedQueueKey],
                arguments: [.init(from: jobID.description), .init(from: Date.now.timeIntervalSince1970)]
            )
        } else {
            _ = try await self.scripts.completed.runScript(
                on: self.redisConnectionPool.wrappedValue,
                keys: [self.configuration.processingQueueKey, jobID.redisKey(for: self)],
                arguments: [.init(from: jobID.description)]
            )
        }
    }

    /// Flag job failed to process
    ///
    /// Removes  job id from processing queue, adds to failed queue
    /// - Parameters:
    ///   - jobID: Job id
    @inlinable
    public func failed(jobID: JobID, error: Error) async throws {
        if self.configuration.retentionPolicy.failedJobs == .retain {
            _ = try await self.scripts.moveToFailed.runScript(
                on: self.redisConnectionPool.wrappedValue,
                keys: [self.configuration.processingQueueKey, self.configuration.failedQueueKey],
                arguments: [.init(from: jobID.description), .init(from: Date.now.timeIntervalSince1970)]
            )
        } else {
            _ = try await self.scripts.failedAndDelete.runScript(
                on: self.redisConnectionPool.wrappedValue,
                keys: [self.configuration.processingQueueKey, jobID.redisKey(for: self)],
                arguments: [.init(from: jobID.description)]
            )

        }
    }

    public func stop() async {
        self.isStopped.store(true, ordering: .relaxed)
    }

    public func shutdownGracefully() async {}

    /// Pop Job off queue and add to pending queue
    /// - Parameter eventLoop: eventLoop to do work on
    /// - Returns: queued job
    @usableFromInline
    func popFirst() async throws -> JobQueueResult<JobID>? {
        let value = try await self.scripts.pop.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [self.configuration.pendingQueueKey, self.configuration.processingQueueKey],
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

    func get(jobID: JobID) async throws -> ByteBuffer? {
        try await self.redisConnectionPool.wrappedValue.get(jobID.redisKey(for: self)).get().byteBuffer
    }

    func delete(jobIDs: [JobID]) async throws {
        _ = try await self.redisConnectionPool.wrappedValue.delete(jobIDs.map { $0.redisKey(for: self) }).get()
    }

    let jobRegistry: JobRegistry
}

extension RedisJobQueue: JobMetadataDriver {
    /// Get job queue metadata
    /// - Parameter key: Metadata key
    /// - Returns: Associated ByteBuffer
    @inlinable
    public func getMetadata(_ key: String) async throws -> ByteBuffer? {
        let key = "\(self.configuration.metadataKeyPrefix)\(key)"
        return try await self.redisConnectionPool.wrappedValue.get(.init(key)).get().byteBuffer
    }

    /// Set job queue metadata
    /// - Parameters:
    ///   - key: Metadata key
    ///   - value: Associated ByteBuffer
    @inlinable
    public func setMetadata(key: String, value: ByteBuffer) async throws {
        let key = "\(self.configuration.metadataKeyPrefix)\(key)"
        try await self.redisConnectionPool.wrappedValue.set(.init(key), to: value).get()
    }

    /// Acquire metadata lock
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - id: Lock identifier
    ///   - expiresIn: When lock will expire
    /// - Returns: If lock was acquired
    @inlinable
    public func acquireLock(key: String, id: ByteBuffer, expiresIn: TimeInterval) async throws -> Bool {
        let key = "\(self.configuration.metadataKeyPrefix)\(key)"
        let response = try await self.scripts.acquireLock.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [.init(key)],
            arguments: [.init(from: id), .init(from: Int(Date.now.timeIntervalSince1970 + expiresIn))]
        )
        return !response.isNull
    }

    /// Release metadata lock
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - id: Lock identifier
    @inlinable
    public func releaseLock(key: String, id: ByteBuffer) async throws {
        let key = "\(self.configuration.metadataKeyPrefix)\(key)"
        _ = try await self.scripts.releaseLock.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [.init(key)],
            arguments: [.init(from: id)]
        )
    }
}

/// extend RedisJobQueue to conform to AsyncSequence
extension RedisJobQueue {
    public typealias Element = JobQueueResult<JobID>
    public struct AsyncIterator: AsyncIteratorProtocol {
        @usableFromInline
        let queue: RedisJobQueue

        @inlinable
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

extension RedisJobQueue: CancellableJobQueue {
    /// Cancels a job
    ///
    /// Removes it from the pending queue
    /// - Parameters:
    ///  - jobID: Job id
    @inlinable
    public func cancel(jobID: JobID) async throws {
        if self.configuration.retentionPolicy.cancelledJobs == .retain {
            _ = try await self.scripts.cancelAndRetain.runScript(
                on: self.redisConnectionPool.wrappedValue,
                keys: [self.configuration.pendingQueueKey, self.configuration.cancelledQueueKey],
                arguments: [.init(from: jobID.description), .init(from: Date.now.timeIntervalSince1970)]
            )
        } else {
            _ = try await self.scripts.cancel.runScript(
                on: self.redisConnectionPool.wrappedValue,
                keys: [self.configuration.pendingQueueKey, jobID.redisKey(for: self)],
                arguments: [.init(from: jobID.description)]
            )
        }
    }
}

extension RedisJobQueue: ResumableJobQueue {
    /// Temporarily remove job from pending queue
    ///
    /// Removes it from the pending queue, adds to paused queue
    /// - Parameters:
    ///  - jobID: Job id
    @inlinable
    public func pause(jobID: JobID) async throws {
        _ = try await self.scripts.pauseResume.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [self.configuration.pendingQueueKey, self.configuration.pausedQueueKey],
            arguments: [.init(from: jobID.description)]
        )
    }

    /// Moved paused job back onto pending queue
    ///
    /// Removes it from the paused queue, adds to pending queue
    /// - Parameters:
    ///  - jobID: Job id
    @inlinable
    public func resume(jobID: JobID) async throws {
        _ = try await self.scripts.pauseResume.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [self.configuration.pausedQueueKey, self.configuration.pendingQueueKey],
            arguments: [.init(from: jobID.description)]
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
    func _zpopmin(
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

    /// Removes and returns the last elements of the list stored at key.
    ///
    /// See [https://redis.io/commands/rpop](https://redis.io/commands/rpop)
    /// - Parameters
    ///    - key: The key of the list to pop from.
    ///    - count: Number of elements to pop
    /// - Returns: The elements that were popped from the list, else `.null`.
    func rpop(from key: RedisKey, count: Int) -> EventLoopFuture<RESPValue> {
        let args = [RESPValue(from: key), RESPValue(from: count)]
        return send(command: "RPOP", with: args)
    }
}
