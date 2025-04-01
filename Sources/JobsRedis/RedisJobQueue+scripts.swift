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
    let moveToProcessing: RedisScript
    let moveToFailed: RedisScript
    let moveToPending: RedisScript
    let pop: RedisScript
    let delete: RedisScript
    let rerunQueue: RedisScript
}

extension RedisJobQueue {
    /// Upload scripts used by swift-job-redis
    static func uploadScripts(redisConnectionPool: RedisConnectionPool, logger: Logger) async throws -> RedisScripts {
        let scripts = try await RedisScripts(
            addToQueue: .init(
                """
                redis.call("SET", KEYS[1], ARGV[1])
                redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
                return redis.status_reply('OK')
                """,
                redisConnectionPool: redisConnectionPool
            ),
            moveToProcessing: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("LPUSH", KEYS[2], ARGV[1])
                return redis.status_reply('OK')
                """,
                redisConnectionPool: redisConnectionPool
            ),
            moveToFailed: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("LPUSH", KEYS[2], ARGV[1])
                return redis.status_reply('OK')
                """,
                redisConnectionPool: redisConnectionPool
            ),
            moveToPending: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("ZADD", KEYS[2], 0, ARGV[2])
                return redis.status_reply('OK')
                """,
                redisConnectionPool: redisConnectionPool
            ),
            pop: .init(
                """
                local values = redis.call("ZPOPMIN", KEYS[1])
                if values[2] == nil then 
                    return nil
                end
                if values[2] > ARGV[1] then
                    redis.call("ZADD", KEYS[1], 0, values[1])
                    return nil
                end
                redis.call("LPUSH", KEYS[2], values[1])
                return values[1]
                """,
                redisConnectionPool: redisConnectionPool
            ),
            delete: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("DEL", KEYS[2])
                return redis.status_reply('OK')
                """,
                redisConnectionPool: redisConnectionPool
            ),
            rerunQueue: .init(
                """
                while true do
                    local value = redis.call("RPOP", KEYS[1])
                    if value == false then
                        break
                    end
                    redis.call("ZADD", KEYS[2], 0, value)
                end
                return redis.status_reply('OK')
                """,
                redisConnectionPool: redisConnectionPool
            )
        )
        logger.debug("AddToQueue script with SHA1 \(scripts.addToQueue.sha1)")
        logger.debug("Move to processing script with SHA1 \(scripts.moveToProcessing.sha1)")
        logger.debug("Move to failed script with SHA1 \(scripts.moveToFailed.sha1)")
        logger.debug("Move to pending script with SHA1 \(scripts.moveToPending.sha1)")
        logger.debug("Pop script with SHA1 \(scripts.pop.sha1)")
        logger.debug("Delete script with SHA1 \(scripts.delete.sha1)")
        logger.debug("Rerun queue script with SHA1 \(scripts.rerunQueue.sha1)")
        return scripts
    }
}
