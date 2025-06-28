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

@usableFromInline
struct RedisScripts: Sendable {
    @usableFromInline
    struct RedisScript: Sendable {
        let script: String
        let sha1: String

        init(_ script: String, redisConnectionPool: RedisConnectionPool) async throws {
            self.script = script
            self.sha1 = try await redisConnectionPool.scriptLoad(script).get()
        }

        @usableFromInline
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
    @usableFromInline
    let failedAndDelete: RedisScript
    @usableFromInline
    let moveToFailed: RedisScript
    let moveToPending: RedisScript
    let pop: RedisScript
    @usableFromInline
    let completed: RedisScript
    @usableFromInline
    let completedAndRetain: RedisScript
    @usableFromInline
    let cancel: RedisScript
    @usableFromInline
    let cancelAndRetain: RedisScript
    @usableFromInline
    let pauseResume: RedisScript
    let rerunQueue: RedisScript
    let rerunSortedSet: RedisScript
    @usableFromInline
    let acquireLock: RedisScript
    @usableFromInline
    let releaseLock: RedisScript
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
            failedAndDelete: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("DEL", KEYS[2])
                return redis.status_reply('OK')
                """,
                redisConnectionPool: redisConnectionPool
            ),
            moveToFailed: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
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
                if #(values) == 0 then 
                    return nil
                end
                if tonumber(values[2]) > tonumber(ARGV[1]) then
                    redis.call("ZADD", KEYS[1], values[2], values[1])
                    return nil
                end
                redis.call("LPUSH", KEYS[2], values[1])
                return values[1]
                """,
                redisConnectionPool: redisConnectionPool
            ),
            completed: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("DEL", KEYS[2])
                return redis.status_reply('OK')
                """,
                redisConnectionPool: redisConnectionPool
            ),
            completedAndRetain: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
                return redis.status_reply('OK')
                """,
                redisConnectionPool: redisConnectionPool
            ),
            cancel: .init(
                """
                if redis.call("ZREM", KEYS[1], ARGV[1]) > 0 then
                    redis.call("DEL", KEYS[2])
                end
                return redis.status_reply('OK')
                """,
                redisConnectionPool: redisConnectionPool
            ),
            cancelAndRetain: .init(
                """
                if redis.call("ZREM", KEYS[1], ARGV[1]) > 0 then
                    redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
                end
                return redis.status_reply('OK')
                """,
                redisConnectionPool: redisConnectionPool
            ),
            pauseResume: .init(
                """
                local score = redis.call("ZSCORE", KEYS[1], ARGV[1])
                if score == nil then
                   return nil
                end
                redis.call("ZREM", KEYS[1], ARGV[1])
                redis.call("ZADD", KEYS[2], score, ARGV[1])
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
            ),
            rerunSortedSet: .init(
                """
                redis.call("ZUNIONSTORE", KEYS[2], 2, KEYS[1], KEYS[2])
                redis.call("DEL", KEYS[1])
                return redis.status_reply('OK')
                """,
                redisConnectionPool: redisConnectionPool
            ),
            acquireLock: .init(
                """
                if redis.call("GET", KEYS[1]) == ARGV[1] then
                    redis.call("EXPIREAT", KEYS[1], ARGV[2])
                    return redis.status_reply('OK')
                else
                    return redis.call("SET", KEYS[1], ARGV[1], "NX", "EXAT", ARGV[2])
                end
                """,
                redisConnectionPool: redisConnectionPool
            ),
            releaseLock: .init(
                """
                if redis.call("GET", KEYS[1]) == ARGV[1] then
                    redis.call("DEL", KEYS[1])
                else
                    return 0
                end
                """,
                redisConnectionPool: redisConnectionPool
            )
        )
        logger.debug("AddToQueue script with SHA1 \(scripts.addToQueue.sha1)")
        logger.debug("Move to processing script with SHA1 \(scripts.moveToProcessing.sha1)")
        logger.debug("Move to failed script with SHA1 \(scripts.moveToFailed.sha1)")
        logger.debug("Move to pending script with SHA1 \(scripts.moveToPending.sha1)")
        logger.debug("Pop script with SHA1 \(scripts.pop.sha1)")
        logger.debug("Delete script with SHA1 \(scripts.completed.sha1)")
        logger.debug("Rerun queue script with SHA1 \(scripts.rerunQueue.sha1)")
        return scripts
    }
}
