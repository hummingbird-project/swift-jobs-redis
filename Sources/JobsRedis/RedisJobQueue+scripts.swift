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

import Logging
@preconcurrency import RediStack

struct RedisScripts {
    struct RedisScript {
        let script: String
        let sha1: String

        init(_ script: String, redisConnectionPool: RedisConnectionPool) async throws {
            self.script = script
            self.sha1 = try await redisConnectionPool.scriptLoad(script).get()
        }

        func runScript(
            on redisConnectionPool: RedisConnectionPool,
            keys: [RedisKey],
            arguments: [RESPValue] = []
        ) async throws -> RESPValue {
            do {
                return try await redisConnectionPool.evalSHA(sha1, keys: keys, arguments: arguments).get()
            } catch let error as RedisError where error.message.hasPrefix("(Redis) NOSCRIPT") {
                _ = try await redisConnectionPool.scriptLoad(script).get()
                return try await runScript(on: redisConnectionPool, keys: keys, arguments: arguments)
            }
        }
    }

    let addToQueue: RedisScript
    let move: RedisScript
    let delete: RedisScript
}

extension RedisJobQueue {
    /// Upload scripts used by swift-job-redis
    static func uploadScripts(redisConnectionPool: RedisConnectionPool, logger: Logger) async throws -> RedisScripts {
        let scripts = try await RedisScripts(
            addToQueue: .init(
                """
                redis.call("SET", KEYS[1], ARGV[1])
                redis.call("LPUSH", KEYS[2], ARGV[2])
                """,
                redisConnectionPool: redisConnectionPool
            ),
            move: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("LPUSH", KEYS[2], ARGV[1])
                """,
                redisConnectionPool: redisConnectionPool
            ),
            delete: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("DEL", KEYS[2])
                """,
                redisConnectionPool: redisConnectionPool
            )
        )
        logger.debug("AddToQueue script with SHA1 \(scripts.addToQueue.sha1)")
        logger.debug("Move script with SHA1 \(scripts.move.sha1)")
        logger.debug("Move script with SHA1 \(scripts.delete.sha1)")
        return scripts
    }
}
