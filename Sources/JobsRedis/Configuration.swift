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

import NIOCore
@preconcurrency import RediStack

extension RedisJobQueue {
    /// Redis Job queue configuration
    public struct Configuration: Sendable {
        /// queue name
        let queueName: String
        /// Pending queue redis key
        let queueKey: RedisKey
        /// Processing queue redis key
        let processingQueueKey: RedisKey
        /// Paused queue redis key
        let pausedQueueKey: RedisKey
        /// Failed queue redis key
        let failedQueueKey: RedisKey
        /// Cancelled queue redis key
        let cancelledQueueKey: RedisKey
        /// Completed queue redis key
        let completedQueueKey: RedisKey
        /// Prefix for metadata
        let metadataKeyPrefix: String
        /// Queue poll time to wait if queue empties
        let pollTime: Duration
        /// Retention policy for jobs
        let retentionPolicy: RetentionPolicy

        public init(
            queueName: String = "_hbJobQueue",
            pollTime: Duration = .milliseconds(100),
            retentionPolicy: RetentionPolicy = .init()
        ) {
            self.queueName = queueName
            self.queueKey = RedisKey("\(queueName).pending")
            self.pausedQueueKey = RedisKey("\(queueName).paused")
            self.processingQueueKey = RedisKey("\(queueName).processing")
            self.failedQueueKey = RedisKey("\(queueName).failed")
            self.cancelledQueueKey = RedisKey("\(queueName).cancelled")
            self.completedQueueKey = RedisKey("\(queueName).completed")
            self.metadataKeyPrefix = "\(queueName).metadata"
            self.pollTime = pollTime
            self.retentionPolicy = retentionPolicy
        }
    }
}
