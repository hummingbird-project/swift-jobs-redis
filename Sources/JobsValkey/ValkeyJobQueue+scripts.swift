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
import NIOCore
import Valkey

@usableFromInline
struct ValkeyScripts: Sendable {
    @usableFromInline
    final class ValkeyScript: @unchecked Sendable {
        let script: String
        var sha1: ByteBuffer

        init(_ script: String, valkeyClient: some ValkeyClientProtocol) throws {
            self.script = script
            self.sha1 = .init()  //try await valkeyClient.scriptLoad(script: script)
        }

        @usableFromInline
        func runScript(
            on valkeyClient: ValkeyClient,
            keys: [ValkeyKey],
            arguments: [String] = []
        ) async throws -> RESPToken {
            do {
                return try await valkeyClient.evalsha(sha1: self.sha1, keys: keys, args: arguments)
            } catch let error as ValkeyClientError where error.message?.hasPrefix("NOSCRIPT") == true {
                self.sha1 = try await valkeyClient.scriptLoad(script: self.script)
                return try await runScript(on: valkeyClient, keys: keys, arguments: arguments)
            }
        }
    }

    let addToQueue: ValkeyScript
    let moveToProcessing: ValkeyScript
    @usableFromInline
    let failedAndDelete: ValkeyScript
    @usableFromInline
    let moveToFailed: ValkeyScript
    let moveToPending: ValkeyScript
    let pop: ValkeyScript
    @usableFromInline
    let completed: ValkeyScript
    @usableFromInline
    let completedAndRetain: ValkeyScript
    @usableFromInline
    let cancel: ValkeyScript
    @usableFromInline
    let cancelAndRetain: ValkeyScript
    @usableFromInline
    let pauseResume: ValkeyScript
    let rerunQueue: ValkeyScript
    let rerunSortedSet: ValkeyScript
    @usableFromInline
    let acquireLock: ValkeyScript
    @usableFromInline
    let releaseLock: ValkeyScript
}

extension ValkeyJobQueue {
    /// Upload scripts used by swift-job-redis
    static func uploadScripts(valkeyClient: ValkeyClient, logger: Logger) async throws -> ValkeyScripts {
        let scripts = try ValkeyScripts(
            addToQueue: .init(
                """
                redis.call("SET", KEYS[1], ARGV[1])
                redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
                return redis.status_reply('OK')
                """,
                valkeyClient: valkeyClient
            ),
            moveToProcessing: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("LPUSH", KEYS[2], ARGV[1])
                return redis.status_reply('OK')
                """,
                valkeyClient: valkeyClient
            ),
            failedAndDelete: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("DEL", KEYS[2])
                return redis.status_reply('OK')
                """,
                valkeyClient: valkeyClient
            ),
            moveToFailed: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
                return redis.status_reply('OK')
                """,
                valkeyClient: valkeyClient
            ),
            moveToPending: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("ZADD", KEYS[2], 0, ARGV[2])
                return redis.status_reply('OK')
                """,
                valkeyClient: valkeyClient
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
                valkeyClient: valkeyClient
            ),
            completed: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("DEL", KEYS[2])
                return redis.status_reply('OK')
                """,
                valkeyClient: valkeyClient
            ),
            completedAndRetain: .init(
                """
                redis.call("LREM", KEYS[1], 0, ARGV[1])
                redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
                return redis.status_reply('OK')
                """,
                valkeyClient: valkeyClient
            ),
            cancel: .init(
                """
                if redis.call("ZREM", KEYS[1], ARGV[1]) > 0 then
                    redis.call("DEL", KEYS[2])
                end
                return redis.status_reply('OK')
                """,
                valkeyClient: valkeyClient
            ),
            cancelAndRetain: .init(
                """
                if redis.call("ZREM", KEYS[1], ARGV[1]) > 0 then
                    redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
                end
                return redis.status_reply('OK')
                """,
                valkeyClient: valkeyClient
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
                valkeyClient: valkeyClient
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
                valkeyClient: valkeyClient
            ),
            rerunSortedSet: .init(
                """
                redis.call("ZUNIONSTORE", KEYS[2], 2, KEYS[1], KEYS[2])
                redis.call("DEL", KEYS[1])
                return redis.status_reply('OK')
                """,
                valkeyClient: valkeyClient
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
                valkeyClient: valkeyClient
            ),
            releaseLock: .init(
                """
                if redis.call("GET", KEYS[1]) == ARGV[1] then
                    redis.call("DEL", KEYS[1])
                else
                    return 0
                end
                """,
                valkeyClient: valkeyClient
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
