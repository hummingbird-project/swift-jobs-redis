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
import Valkey

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Parameters for Cleanup job
public struct ValkeyJobCleanupParameters: Sendable & Codable {
    let completedJobs: ValkeyJobQueue.JobCleanup
    let failedJobs: ValkeyJobQueue.JobCleanup
    let cancelledJobs: ValkeyJobQueue.JobCleanup

    ///  Initialize ValkeyJobCleanupParameters
    /// - Parameters:
    ///   - completedJobs: What to do with completed jobs
    ///   - failedJobs: What to do with failed jobs
    ///   - cancelledJobs: What to do with cancelled jobs
    public init(
        completedJobs: ValkeyJobQueue.JobCleanup = .doNothing,
        failedJobs: ValkeyJobQueue.JobCleanup = .doNothing,
        cancelledJobs: ValkeyJobQueue.JobCleanup = .doNothing
    ) {
        self.completedJobs = completedJobs
        self.failedJobs = failedJobs
        self.cancelledJobs = cancelledJobs
    }
}

extension ValkeyJobQueue {
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
    /// Use this with the ``/Jobs/JobSchedule`` to schedule a cleanup of
    /// failed, cancelled or completed jobs
    public var cleanupJob: JobName<ValkeyJobCleanupParameters> {
        .init("_Jobs_RedisCleanup_\(self.configuration.queueName)")
    }

    /// register clean up job on queue
    func registerCleanupJob() {
        self.registerJob(
            JobDefinition(name: cleanupJob, parameters: ValkeyJobCleanupParameters.self, retryStrategy: .dontRetry) { parameters, context in
                try await self.cleanup(
                    pendingJobs: .doNothing,
                    processingJobs: .doNothing,
                    completedJobs: parameters.completedJobs,
                    failedJobs: parameters.failedJobs,
                    cancelledJobs: parameters.cancelledJobs
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
    ///   - pendingJobs: What to do with jobs tagged as pending
    ///   - processingJobs: What to do with jobs tagged as processing
    ///   - completedJobs: What to do with jobs tagged as completed
    ///   - failedJobs: What to do with jobs tagged as failed
    ///   - cancelledJobs: What to do with jobs tagged as cancelled
    /// - Throws:
    public func cleanup(
        pendingJobs: PendingJobCleanup = .doNothing,
        processingJobs: ProcessingJobCleanup = .doNothing,
        completedJobs: JobCleanup = .doNothing,
        failedJobs: JobCleanup = .doNothing,
        cancelledJobs: JobCleanup = .doNothing
    ) async throws {
        try await self.cleanupPendingQueue(queueKey: self.configuration.pendingQueueKey, cleanup: pendingJobs)
        // there shouldn't be any on the processing list, but if there are we should do something with them
        try await self.cleanupSet(key: self.configuration.processingQueueKey, cleanup: processingJobs)
        try await self.cleanupSortedSet(key: self.configuration.failedQueueKey, cleanup: failedJobs)
        try await self.cleanupSortedSet(key: self.configuration.cancelledQueueKey, cleanup: cancelledJobs)
        try await self.cleanupSortedSet(key: self.configuration.completedQueueKey, cleanup: completedJobs)
    }

    /// What to do with set at initialization
    func cleanupSet(key: ValkeyKey, cleanup: ProcessingJobCleanup) async throws {
        switch cleanup.rawValue {
        case .remove:
            self.logger.debug("Remove set", metadata: ["key": .stringConvertible(key)])
            try await self.removeSet(key: key)
        case .rerun:
            self.logger.debug("Rerun set", metadata: ["key": .stringConvertible(key)])
            try await self.rerunSet(key: key)
        case .doNothing:
            break
        }
    }

    /// What to do with set at initialization
    func cleanupSortedSet(key: ValkeyKey, cleanup: JobCleanup) async throws {
        switch cleanup.rawValue {
        case .remove(let maxAge):
            self.logger.debug("Remove queue", metadata: ["key": .stringConvertible(key)])
            try await self.removeSortedSet(key: key, maxAge: maxAge)
        case .rerun:
            self.logger.debug("Rerun queue", metadata: ["key": .stringConvertible(key)])
            try await self.rerunSortedSet(key: key)
        case .doNothing:
            break
        }
    }

    /// What to do with the pending queue at initialization
    func cleanupPendingQueue(queueKey: ValkeyKey, cleanup: PendingJobCleanup) async throws {
        switch cleanup.rawValue {
        case .remove(let maxAge):
            self.logger.debug("Remove pending queue", metadata: ["key": .stringConvertible(queueKey)])
            try await self.removeSortedSet(key: queueKey, maxAge: maxAge)
        case .doNothing:
            break
        }
    }

    /// Push all the entries from list back onto the main list.
    func rerunSet(key: ValkeyKey) async throws {
        _ = try await self.scripts.rerunQueue.runScript(
            on: self.valkeyClient,
            keys: [key, self.configuration.pendingQueueKey],
            arguments: []
        )
    }

    /// Delete all entries from queue
    func removeSet(key: ValkeyKey) async throws {
        // Cannot use a script for this as it edits keys that are not input keys
        while true {
            guard let response = try await self.valkeyClient.rpop(key, count: 100) else {
                break
            }
            guard let jobIDs = try? [JobID](fromRESP: response) else {
                throw ValkeyQueueError.unexpectedValkeyKeyType
            }
            try await self.delete(jobIDs: jobIDs)
        }
    }

    /// Push all the entries from sorted set back onto the pending sorted set.
    func rerunSortedSet(key: ValkeyKey) async throws {
        _ = try await self.scripts.rerunSortedSet.runScript(
            on: self.valkeyClient,
            keys: [key, self.configuration.pendingQueueKey],
            arguments: []
        )
    }

    /// Delete all entries from queue older than specified date
    func removeSortedSet(key: ValkeyKey, maxAge: Duration?) async throws {
        let date: Date =
            if let maxAge {
                .now - Double(maxAge.components.seconds)
            } else {
                .distantFuture
            }
        // Get number of keys we should pop
        while true {
            // get count of keys older than date
            let count = try await self.valkeyClient.zcount(key, min: -Double.infinity, max: date.timeIntervalSince1970)
            guard count > 0 else {
                break
            }
            // Pop a maximum of 100 keys at one time
            let chunk = Swift.min(count, 100)
            let values = try await self.valkeyClient.zpopmin(key, count: chunk)
            guard values.first != nil else {
                break
            }
            var index = 0
            while index < values.count {
                guard values[index].score <= date.timeIntervalSince1970 else {
                    break
                }
                index += 1
            }
            if index < values.count {
                // if we broke out of the loop before reaching the end we found a value which shouldnt be
                // deleted. Delete everything up until that point and add the remaining values back into the
                // sorted set
                let jobIDs = values[..<index].map { JobID(buffer: $0.value) }
                try await self.delete(jobIDs: jobIDs)
                _ = try await self.valkeyClient.zadd(key, data: values[index...].map { .init(score: $0.score, member: $0.value) })
            } else {
                // delete all jobIDs returned by zpopmin
                let jobIDs = values.map { JobID(buffer: $0.value) }
                try await self.delete(jobIDs: jobIDs)
            }
        }
    }
}
