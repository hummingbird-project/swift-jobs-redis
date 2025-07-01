//===----------------------------------------------------------------------===//
//
// This source file is part of the Hummingbird server framework project
//
// Copyright (c) 2025 the Hummingbird authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See hummingbird/CONTRIBUTORS.txt for the list of Hummingbird authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

extension RedisJobQueue {
    /// Data rentension policy
    public struct RetentionPolicy: Sendable {
        /// Data retention policy
        public struct RetainData: Equatable, Sendable {
            enum Policy {
                case retain
                case doNotRetain
            }

            let rawValue: Policy
            /// Retain task
            public static var retain: RetainData { RetainData(rawValue: .retain) }
            /// Never retain any data
            public static var doNotRetain: RetainData { RetainData(rawValue: .doNotRetain) }
        }

        /// Jobs with status completed
        public var completedJobs: RetainData
        /// Jobs with status failed
        public var failedJobs: RetainData
        /// Jobs with status cancelled
        public var cancelledJobs: RetainData

        public init(
            completedJobs: RetainData = .doNotRetain,
            failedJobs: RetainData = .retain,
            cancelledJobs: RetainData = .doNotRetain
        ) {
            self.completedJobs = completedJobs
            self.failedJobs = failedJobs
            self.cancelledJobs = cancelledJobs
        }
    }
}
