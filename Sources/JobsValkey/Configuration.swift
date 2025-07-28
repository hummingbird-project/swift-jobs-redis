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
import Valkey

extension ValkeyJobQueue {
    /// Redis Job queue configuration
    public struct Configuration: Sendable {
        /// queue name
        public let queueName: String
        /// Pending queue redis key
        public let pendingQueueKey: ValkeyKey
        /// Processing queue redis key
        public let processingQueueKey: ValkeyKey
        /// Paused queue redis key
        public let pausedQueueKey: ValkeyKey
        /// Failed queue redis key
        public let failedQueueKey: ValkeyKey
        /// Cancelled queue redis key
        public let cancelledQueueKey: ValkeyKey
        /// Completed queue redis key
        public let completedQueueKey: ValkeyKey
        /// Prefix for metadata
        public let metadataKeyPrefix: String
        /// Queue poll time to wait if queue empties
        public let pollTime: Duration
        /// Retention policy for jobs
        public let retentionPolicy: RetentionPolicy

        public init(
            queueName: String = "_hbJobQueue",
            pollTime: Duration = .milliseconds(100),
            retentionPolicy: RetentionPolicy = .init()
        ) {
            self.queueName = queueName
            self.pendingQueueKey = ValkeyKey("\(queueName).pending")
            self.pausedQueueKey = ValkeyKey("\(queueName).paused")
            self.processingQueueKey = ValkeyKey("\(queueName).processing")
            self.failedQueueKey = ValkeyKey("\(queueName).failed")
            self.cancelledQueueKey = ValkeyKey("\(queueName).cancelled")
            self.completedQueueKey = ValkeyKey("\(queueName).completed")
            self.metadataKeyPrefix = "\(queueName).metadata."
            self.pollTime = pollTime
            self.retentionPolicy = retentionPolicy
        }
    }
}
