import NIOCore
import RediStack

extension RedisClient {
    /// Invoke the execution of a server-side Lua script.
    ///
    /// See [https://redis.io/commands/eval](https://redis.io/commands/eval)
    /// - Parameters
    ///   - script: The script to evaluate
    ///   - keys: Keys accessed by script
    ///   - arguments: Arguments of script
    /// - Returns: The message sent with the command.
    public func eval(_ script: String, keys: [RedisKey], arguments: [RESPValue]) -> EventLoopFuture<RESPValue> {
        var args: [RESPValue] = [.init(from: script)]
        args.append(.init(from: keys.count))
        args.append(contentsOf: keys.map { .init(from: $0) })
        args.append(contentsOf: arguments)
        return send(command: "EVAL", with: args)
    }

    /// This is a read-only variant of the EVAL command that cannot execute commands that modify data.
    ///
    /// See [https://redis.io/commands/eval_ro](https://redis.io/commands/eval_ro)
    /// - Parameters
    ///   - script: The script to evaluate
    ///   - keys: Keys accessed by script
    ///   - arguments: Arguments of script
    /// - Returns: The message sent with the command.
    public func evalReadOnly(_ script: String, keys: [RedisKey], arguments: [RESPValue]) -> EventLoopFuture<RESPValue> {
        var args: [RESPValue] = [.init(from: script)]
        args.append(.init(from: keys.count))
        args.append(contentsOf: keys.map { .init(from: $0) })
        args.append(contentsOf: arguments)
        return send(command: "EVAL_RO", with: args)
    }

    /// Evaluate a script from the server's cache by its SHA1 digest.
    ///
    /// See [https://redis.io/commands/evalsha](https://redis.io/commands/evalsha)
    /// - Parameters
    ///   - scriptSHA1: The script to evaluate
    ///   - keys: Keys accessed by script
    ///   - arguments: Arguments of script
    /// - Returns: The message sent with the command.
    public func evalSHA(_ scriptSHA1: String, keys: [RedisKey], arguments: [RESPValue]) -> EventLoopFuture<RESPValue> {
        var args: [RESPValue] = [.init(from: scriptSHA1)]
        args.append(.init(from: keys.count))
        args.append(contentsOf: keys.map { .init(from: $0) })
        args.append(contentsOf: arguments)
        return send(command: "EVALSHA", with: args)
    }

    /// This is a read-only variant of the EVALSHA command that cannot execute commands that modify data.
    ///
    /// See [https://redis.io/commands/evalsha_ro](https://redis.io/commands/evalsha_ro)
    /// - Parameters
    ///   - scriptSHA1: The script to evaluate
    ///   - keys: Keys accessed by script
    ///   - arguments: Arguments of script
    /// - Returns: The message sent with the command.
    public func evalSHAReadOnly(_ scriptSHA1: String, keys: [RedisKey], arguments: [RESPValue]) -> EventLoopFuture<RESPValue> {
        var args: [RESPValue] = [.init(from: scriptSHA1)]
        args.append(.init(from: keys.count))
        args.append(contentsOf: keys.map { .init(from: $0) })
        args.append(contentsOf: arguments)
        return send(command: "EVALSHA_RO", with: args)
    }

    /// Returns information about the existence of a script in the script cache.
    ///
    /// This command accepts one or more SHA1 digests and returns a list of ones or zeros to signal
    /// if the scripts are already defined or not inside the script cache. This can be useful before
    /// a pipelining operation to ensure that scripts are loaded (and if not, to load them using SCRIPT
    /// LOAD) so that the pipelining operation can be performed solely using EVALSHA instead of EVAL
    /// to save bandwidth.
    ///
    /// See [https://redis.io/commands/script-exists](https://redis.io/commands/script-exists)
    /// - Parameters
    ///   - script: The script to evaluate
    ///   - keys: Keys accessed by script
    ///   - arguments: Arguments of script
    /// - Returns: The message sent with the command.
    public func scriptExists(_ sha1: String) -> EventLoopFuture<Int> {
        let args: [RESPValue] = [.init(from: "EXISTS"), .init(from: sha1)]
        return send(command: "SCRIPT", with: args).tryConverting()
    }

    /// Returns information about the existence of the scripts in the script cache.
    ///
    /// This command accepts one or more SHA1 digests and returns a list of ones or zeros to signal
    /// if the scripts are already defined or not inside the script cache. This can be useful before
    /// a pipelining operation to ensure that scripts are loaded (and if not, to load them using SCRIPT
    /// LOAD) so that the pipelining operation can be performed solely using EVALSHA instead of EVAL
    /// to save bandwidth.
    ///
    /// See [https://redis.io/commands/script-exists](https://redis.io/commands/script-exists)
    /// - Parameters
    ///   - script: The script to evaluate
    ///   - keys: Keys accessed by script
    ///   - arguments: Arguments of script
    /// - Returns: The message sent with the command.
    public func scriptsExists(_ sha1: String) -> EventLoopFuture<[Int]> {
        let args: [RESPValue] = [.init(from: "EXISTS"), .init(from: sha1)]
        return send(command: "SCRIPT", with: args).tryConverting()
    }

    /// Flush the Lua scripts cache.
    ///
    /// By default, SCRIPT FLUSH will synchronously flush the cache. Starting with Redis 6.2, setting
    /// the lazyfree-lazy-user-flush configuration directive to "yes" changes the default flush mode to asynchronous.
    ///
    /// See [https://redis.io/commands/script-flush](https://redis.io/commands/script-flush)
    /// - Parameters
    ///   - script: The script to evaluate
    ///   - keys: Keys accessed by script
    ///   - arguments: Arguments of script
    /// - Returns: The message sent with the command.
    public func scriptFlush(_ flush: RedisScriptFlush?) -> EventLoopFuture<RESPValue> {
        let args: [RESPValue] = flush.map { [.init(from: "FLUSH"), .init(from: $0.rawValue)] } ?? [.init(from: "FLUSH")]
        return send(command: "SCRIPT", with: args)
    }

    /// Kills the currently executing EVAL script, assuming no write operation was yet performed by the script.
    ///
    /// This command is mainly useful to kill a script that is running for too much time(for instance, because it
    /// entered an infinite loop because of a bug). The script will be killed, and the client currently blocked
    /// into EVAL will see the command returning with an error.
    ///
    /// See [https://redis.io/commands/script-kill](https://redis.io/commands/script-kill)
    /// - Parameters
    ///   - script: The script to evaluate
    ///   - keys: Keys accessed by script
    ///   - arguments: Arguments of script
    /// - Returns: The message sent with the command.
    public func scriptKill() -> EventLoopFuture<RESPValue> {
        send(command: "SCRIPT", with: [.init(from: "KILL")])
    }

    /// Load a script into the scripts cache, without executing it. After the specified command is loaded
    /// into the script cache it will be callable using EVALSHA with the correct SHA1 digest of the script,
    /// exactly like after the first successful invocation of EVAL.
    ///
    /// See [https://redis.io/commands/script-load](https://redis.io/commands/script-load)
    /// - Parameters
    ///   - script: The script to evaluate
    ///   - keys: Keys accessed by script
    ///   - arguments: Arguments of script
    /// - Returns: The message sent with the command.
    public func scriptLoad(_ script: String) -> EventLoopFuture<String> {
        let args: [RESPValue] = [.init(from: "LOAD"), .init(from: script)]
        return send(command: "SCRIPT", with: args).tryConverting()
    }
}

extension EventLoopFuture where Value == RESPValue {
    /// Attempts to convert the resolved RESPValue to the desired type.
    ///
    /// This method is intended to be used much like a precondition in synchronous code, where a value is expected to be available from the `RESPValue`.
    /// - Important: If the `RESPValueConvertible` initializer fails, then the `NIO.EventLoopFuture` will fail.
    /// - Parameter to: The desired type to convert to.
    /// - Throws: `RedisClientError.failedRESPConversion(to:)`
    /// - Returns: A `NIO.EventLoopFuture` that resolves a value of the desired type or fails if the conversion does.
    @usableFromInline
    internal func tryConverting<T: RESPValueConvertible>(
        to type: T.Type = T.self,
        file: StaticString = #file,
        line: UInt = #line
    ) -> EventLoopFuture<T> {
        self.flatMapThrowing {
            guard let value = T(fromRESP: $0) else {
                throw RedisClientError.failedRESPConversion(to: type)
            }
            return value
        }
    }
}

/// Script flush mode
public enum RedisScriptFlush: String {
    case async = "ASYNC"
    case sync = "SYNC"
}
