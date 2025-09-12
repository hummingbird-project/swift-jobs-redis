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

extension ValkeyJobQueue {
    static let FunctionVersion = 1
    /// Upload Valkey JobQueue functions to server
    public func loadFunctions() async throws {
        // Only load function if they don't exist or the version number is different
        do {
            let version = try await self.valkeyClient.fcall(function: "swiftjobs_version").decode(as: Int.self)
            if version == Self.FunctionVersion {
                return
            }
        } catch let error as ValkeyClientError where error.errorCode == .commandError {
        }
        try await self.loadFunctions.acquire {
            _ = try await self.valkeyClient.functionLoad(
                replace: true,
                functionCode: """
                    #!lua name=_swift_jobs_valkey

                    local function version()
                        return 1
                    end

                    local function pop(keys, args)
                        local values = redis.call("ZPOPMIN", keys[1])
                        if #(values) == 0 then 
                            return nil
                        end
                        if tonumber(values[2]) > tonumber(args[1]) then
                            redis.call("ZADD", keys[1], values[2], values[1])
                            return nil
                        end
                        redis.call("LPUSH", keys[2], values[1])
                        return values[1]
                    end

                    local function cancel(keys, args)
                        if redis.call("ZREM", keys[1], args[1]) > 0 then
                            redis.call("DEL", keys[2])
                        end
                        return redis.status_reply('OK')
                    end

                    local function cancelAndRetain(keys, args)
                        if redis.call("ZREM", keys[1], args[1]) > 0 then
                            redis.call("ZADD", keys[2], args[2], args[1])
                        end
                        return redis.status_reply('OK')
                    end

                    local function pauseResume(keys, args)
                        local score = redis.call("ZSCORE", keys[1], args[1])
                        if score == nil then
                            return nil
                        end
                        redis.call("ZREM", keys[1], args[1])
                        redis.call("ZADD", keys[2], score, args[1])
                        return redis.status_reply('OK')
                    end

                    local function rerunQueue(keys, args)
                        while true do
                            local value = redis.call("RPOP", keys[1])
                            if value == false then
                                break
                            end
                            redis.call("ZADD", keys[2], 0, value)
                        end
                        return redis.status_reply('OK')
                    end

                    server.register_function('swiftjobs_version', version)
                    server.register_function('swiftjobs_pop', pop)
                    server.register_function('swiftjobs_cancel', cancel)
                    server.register_function('swiftjobs_cancelAndRetain', cancelAndRetain)
                    server.register_function('swiftjobs_pauseResume', pauseResume)
                    server.register_function('swiftjobs_rerunQueue', rerunQueue)
                    """
            )
        }
    }
}
