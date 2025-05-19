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
@preconcurrency import RediStack

/// Parameters for Cleanup job
public struct RedisJobCleanupParameters: Sendable & Codable {
    let failedJobs: RedisJobQueue.JobCleanup
    let completedJobs: RedisJobQueue.JobCleanup
    let cancelledJobs: RedisJobQueue.JobCleanup

    public init(
        failedJobs: RedisJobQueue.JobCleanup = .doNothing,
        completedJobs: RedisJobQueue.JobCleanup = .doNothing,
        cancelledJobs: RedisJobQueue.JobCleanup = .doNothing
    ) {
        self.failedJobs = failedJobs
        self.completedJobs = completedJobs
        self.cancelledJobs = cancelledJobs
    }
}

extension RedisJobQueue {
    /// how to cleanup a job
    public struct JobCleanup: Sendable, Codable {
        enum RawValue: Codable {
            case doNothing
            case rerun
            case remove(maxAge: Duration?)
        }
        let rawValue: RawValue

        public static var doNothing: Self { .init(rawValue: .doNothing) }
        public static var rerun: Self { .init(rawValue: .rerun) }
        public static var remove: Self { .init(rawValue: .remove(maxAge: nil)) }
        public static func remove(maxAge: Duration) -> Self { .init(rawValue: .remove(maxAge: maxAge)) }
    }

    /// how to cleanup a currently processing job
    public struct ProcessingJobCleanup: Sendable, Codable, Equatable {
        enum RawValue: Codable, Equatable {
            case doNothing
            case rerun
            case remove
        }
        let rawValue: RawValue

        public static var doNothing: Self { .init(rawValue: .doNothing) }
        public static var rerun: Self { .init(rawValue: .rerun) }
        public static var remove: Self { .init(rawValue: .remove) }
    }

    /// how to cleanup a currently processing job
    public struct PendingJobCleanup: Sendable, Codable, Equatable {
        enum RawValue: Codable, Equatable {
            case doNothing
            case remove(maxAge: Duration?)
        }
        let rawValue: RawValue

        public static var doNothing: Self { .init(rawValue: .doNothing) }
        public static var remove: Self { .init(rawValue: .remove(maxAge: nil)) }
        public static func remove(maxAge: Duration) -> Self { .init(rawValue: .remove(maxAge: maxAge)) }
    }

    /// clean up job name.
    ///
    /// Use this with the ``JobSchedule`` to schedule a cleanup of
    /// failed, cancelled or completed jobs
    public var cleanupJob: JobName<RedisJobCleanupParameters> {
        .init("_RedisJobCleanup_\(self.configuration.queueName)")
    }

    /// register clean up job on queue
    func registerCleanupJob() {
        self.registerJob(
            JobDefinition(name: cleanupJob, parameters: RedisJobCleanupParameters.self, retryStrategy: .dontRetry) { parameters, context in
                try await self.cleanup(
                    failedJobs: parameters.failedJobs,
                    processingJobs: .doNothing,
                    pendingJobs: .doNothing,
                    cancelledJobs: parameters.cancelledJobs,
                    completedJobs: parameters.completedJobs
                )
            }
        )
    }

    /// Cleanup job queues
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
        processingJobs: ProcessingJobCleanup = .doNothing,
        pendingJobs: PendingJobCleanup = .doNothing,
        cancelledJobs: JobCleanup = .doNothing,
        completedJobs: JobCleanup = .doNothing
    ) async throws {
        try await self.cleanupPendingQueue(queueKey: self.configuration.queueKey, cleanup: pendingJobs)
        // there shouldn't be any on the processing list, but if there are we should do something with them
        try await self.cleanupSet(key: self.configuration.processingQueueKey, cleanup: processingJobs)
        try await self.cleanupSortedSet(key: self.configuration.failedQueueKey, cleanup: failedJobs)
        try await self.cleanupSortedSet(key: self.configuration.cancelledQueueKey, cleanup: cancelledJobs)
        try await self.cleanupSortedSet(key: self.configuration.completedQueueKey, cleanup: completedJobs)
    }

    /// What to do with set at initialization
    func cleanupSet(key: RedisKey, cleanup: ProcessingJobCleanup) async throws {
        switch cleanup.rawValue {
        case .remove:
            try await self.removeSet(key: key)
        case .rerun:
            try await self.rerunSet(key: key)
        case .doNothing:
            break
        }
    }

    /// What to do with set at initialization
    func cleanupSortedSet(key: RedisKey, cleanup: JobCleanup) async throws {
        switch cleanup.rawValue {
        case .remove(let maxAge):
            try await self.removeSortedSet(key: key, maxAge: maxAge)
        case .rerun:
            try await self.rerunSortedSet(key: key)
        case .doNothing:
            break
        }
    }

    /// What to do with the pending queue at initialization
    func cleanupPendingQueue(queueKey: RedisKey, cleanup: PendingJobCleanup) async throws {
        switch cleanup.rawValue {
        case .remove(let maxAge):
            try await self.removeSortedSet(key: queueKey, maxAge: maxAge)
        case .doNothing:
            break
        }
    }

    /// Push all the entries from list back onto the main list.
    func rerunSet(key: RedisKey) async throws {
        _ = try await self.scripts.rerunQueue.runScript(
            on: self.redisConnectionPool.wrappedValue,
            keys: [key, self.configuration.queueKey],
            arguments: []
        )
    }

    /// Delete all entries from queue
    func removeSet(key: RedisKey) async throws {
        // Cannot use a script for this as it edits keys that are not input keys
        while true {
            let key = try await self.redisConnectionPool.wrappedValue.rpop(from: key).get()
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

    /// Push all the entries from sorted set back onto the pending sorted set.
    func rerunSortedSet(key: RedisKey) async throws {
        _ = try await self.redisConnectionPool.wrappedValue.zunionstore(as: self.configuration.queueKey, sources: [self.configuration.queueKey, key])
            .get()
    }

    /// Delete all entries from queue older than specified date
    func removeSortedSet(key: RedisKey, maxAge: Duration?) async throws {
        let date: Date =
            if let maxAge {
                .now - Double(maxAge.components.seconds)
            } else {
                .distantFuture
            }
        // Get number of keys we should pop
        while true {
            // get count of keys older than date
            let count = try await self.redisConnectionPool.wrappedValue.zcount(of: key, withMaximumScoreOf: .inclusive(date.timeIntervalSince1970))
                .get()
            guard count > 0 else {
                break
            }
            // Pop a maximum of 100 keys at one time
            let chunk = Swift.min(count, 100)
            let values = try await self.redisConnectionPool.wrappedValue._zpopmin(count: chunk, from: key).get()
            guard values.first != nil else {
                break
            }
            var index = 0
            while index < values.count {
                guard values[index].1 <= date.timeIntervalSince1970 else {
                    break
                }
                guard let jobID = JobID(fromRESP: values[index].0) else {
                    throw RedisQueueError.unexpectedRedisKeyType
                }
                try await self.delete(jobID: jobID)
                index += 1
            }
            if index < values.count {
                _ = try await self.redisConnectionPool.wrappedValue.zadd(values[index...].map { (element: $0.0, score: $0.1) }, to: key).get()
            }
        }
    }
}
