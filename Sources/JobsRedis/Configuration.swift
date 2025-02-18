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
        let queueKey: RedisKey
        let processingQueueKey: RedisKey
        let failedQueueKey: RedisKey
        let pollTime: Duration

        public init(
            queueKey: String = "_hbJobQueue",
            pollTime: Duration = .milliseconds(100)
        ) {
            self.queueKey = RedisKey(queueKey)
            self.processingQueueKey = RedisKey("\(queueKey)Processing")
            self.failedQueueKey = RedisKey("\(queueKey)Failed")
            self.pollTime = pollTime
        }
    }
}
