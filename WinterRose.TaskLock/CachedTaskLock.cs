using System.Collections.Concurrent;

namespace WinterRose.TaskLock
{
    /// <summary>
    /// Wraps <see cref="TaskLock{TKey, TResult}"/> to additionally cache successful
    /// results beyond the lifetime of the in-flight request that produced them.
    /// <br/><br/>
    /// Without this wrapper, <see cref="TaskLock{TKey, TResult}"/> only de-duplicates
    /// work that is concurrently in progress: as soon as a task completes it's removed,
    /// so the next caller (even a millisecond later) triggers a brand new factory call.
    /// <see cref="CachedTaskLock{TKey, TResult}"/> keeps the completed result around so
    /// callers can skip regeneration entirely, until the entry has gone unused for
    /// longer than <see cref="IdleTimeout"/>, at which point it's evicted.
    /// </summary>
    /// <remarks>
    /// - Expiration is sliding/idle-based, not absolute: every successful access
    ///   (cache hit OR the completion of a fresh generation) refreshes the entry's
    ///   last-access time. An entry is only evicted once it has gone unused for the
    ///   full idle timeout.<br/>
    /// - Only successful results are cached. Failures/cancellations are never stored;
    ///   the next call after a failure will simply attempt generation again, same as
    ///   plain <see cref="TaskLock{TKey, TResult}"/>.<br/>
    /// - Concurrent callers for the same key while generation is in progress still only
    ///   trigger one factory invocation, via the underlying <see cref="TaskLock{TKey, TResult}"/>.<br/>
    /// - Thread-safe. Single-process, in-memory, matching <see cref="TaskLock{TKey, TResult}"/>.<br/>
    /// - Implements <see cref="IDisposable"/> because it owns a background sweep timer;
    ///   dispose it when you're done with the cache (e.g. on app shutdown).
    /// </remarks>
    /// <typeparam name="TKey">Identity of the unit of work / cache key.</typeparam>
    /// <typeparam name="TResult">What the work produces, and what gets cached.</typeparam>
    public sealed class CachedTaskLock<TKey, TResult> : IDisposable
        where TKey : notnull
    {
        private sealed class CacheEntry
        {
            public required TResult Value { get; init; }
            public long LastAccessedTicks; // Environment.TickCount64, written via Interlocked.Exchange
            public long CreatedTicks; // Environment.TickCount64

            public void Touch() => Interlocked.Exchange(ref LastAccessedTicks, Environment.TickCount64);

            public bool IsExpired(TimeSpan idleTimeout, TimeSpan absoluteTimeout)
            {
                var now = Environment.TickCount64;

                var idleElapsedMs = now - Interlocked.Read(ref LastAccessedTicks);
                if (idleElapsedMs >= idleTimeout.TotalMilliseconds)
                    return true;

                var absoluteElapsedMs = now - Interlocked.Read(ref CreatedTicks);
                return absoluteElapsedMs >= absoluteTimeout.TotalMilliseconds;
            }
        }

        private readonly TaskLock<TKey, TResult> _taskLock = new();
        private readonly ConcurrentDictionary<TKey, CacheEntry> _cache = new();
        private readonly Timer _sweepTimer;
        private readonly object _disposeLock = new();
        private bool _disposed;

        /// <summary>
        /// How long a cache entry may go without being accessed before it becomes
        /// eligible for eviction. Sliding: any access (hit or a fresh completion)
        /// resets this clock for that entry.
        /// </summary>
        public TimeSpan IdleTimeout { get; }

        /// <summary>
        /// Maximum lifetime of a cache entry. Unlike <see cref="IdleTimeout"/>,
        /// this timeout is absolute and is not extended by accesses.
        /// </summary>
        public TimeSpan AbsoluteTimeout { get; }

        /// <summary>
        /// How often the background sweep checks for expired entries. Defaults to
        /// a fraction of <paramref name="idleTimeout"/> if not specified, so expiry
        /// is reasonably prompt without being a tight busy-loop for long timeouts.
        /// </summary>
        /// <param name="idleTimeout">Idle-time-since-last-access after which an entry is evicted.</param>
        /// <param name="sweepInterval">
        /// How often to scan for expired entries. If null, defaults to
        /// <paramref name="idleTimeout"/> / 4, clamped to [1s, 5min].
        /// </param>
        public CachedTaskLock(
            TimeSpan idleTimeout,
            TimeSpan? absoluteTimeout = null,
            TimeSpan? sweepInterval = null)
        {
            if (idleTimeout <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(idleTimeout), "Idle timeout must be positive.");

            if (absoluteTimeout <= idleTimeout)
                throw new ArgumentOutOfRangeException(
                    nameof(absoluteTimeout),
                    "Absolute timeout must be longer than the idle timeout.");

            IdleTimeout = idleTimeout;
            AbsoluteTimeout = absoluteTimeout ?? TimeSpan.FromHours(1);

            var interval = sweepInterval ?? ClampInterval(TimeSpan.FromTicks(idleTimeout.Ticks / 4));
            _sweepTimer = new Timer(_ => Sweep(), null, interval, interval);
        }

        private static TimeSpan ClampInterval(TimeSpan proposed)
        {
            var min = TimeSpan.FromSeconds(1);
            var max = TimeSpan.FromMinutes(5);
            if (proposed < min) return min;
            if (proposed > max) return max;
            return proposed;
        }

        /// <summary>
        /// Get the cached result for <paramref name="key"/> if present and not expired;
        /// otherwise generate it (de-duplicated against concurrent callers via the
        /// underlying <see cref="TaskLock{TKey, TResult}"/>), cache the result, and return it.
        /// </summary>
        /// <param name="key">Identity of the work / cache entry.</param>
        /// <param name="factory">Produces the result on a cache miss.</param>
        /// <param name="cancellationToken">
        /// See <see cref="TaskLock{TKey, TResult}.GetOrAddAsync"/> for cancellation semantics
        /// on the underlying in-flight de-duplication. Cancelling does not affect cache entries.
        /// </param>
        public async Task<TResult> GetOrAddAsync(
            TKey key,
            Func<CancellationToken, Task<TResult>> factory,
            CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_cache.TryGetValue(key, out var existing))
            {
                existing.Touch();
                return existing.Value;
            }

            var result = await _taskLock.GetOrAddAsync(key, factory, cancellationToken).ConfigureAwait(false);

            var entry = _cache.AddOrUpdate(
                key,
                _ =>
                {
                    var now = Environment.TickCount64;
                    return new CacheEntry
                    {
                        Value = result,
                        CreatedTicks = now,
                        LastAccessedTicks = now
                    };
                },
                (_, existingEntry) => existingEntry);

            entry.Touch();
            return entry.Value;
        }

        /// <summary>
        /// Removes a specific entry from the cache immediately, regardless of its
        /// idle time. Does not affect any generation currently in progress for that key.
        /// </summary>
        public bool Invalidate(TKey key) => _cache.TryRemove(key, out _);

        /// <summary>Removes all cached entries immediately. Does not affect in-flight generations.</summary>
        public void Clear() => _cache.Clear();

        /// <summary>True if a non-expired cached value currently exists for this key. Informational/racy.</summary>
        public bool IsCached(TKey key) => _cache.ContainsKey(key);

        /// <summary>True if work is currently being done for this key. Informational/racy.</summary>
        public bool IsInProgress(TKey key) => _taskLock.IsInFlight(key);

        private void Sweep()
        {
            foreach (var kvp in _cache)
            {
                if (kvp.Value.IsExpired(IdleTimeout, AbsoluteTimeout))
                {
                    // Only removes the entry if this exact key/value pair is still cached.
                    _cache.TryRemove(new KeyValuePair<TKey, CacheEntry>(kvp.Key, kvp.Value));
                }
            }
        }

        public void Dispose()
        {
            lock (_disposeLock)
            {
                if (_disposed) return;
                _disposed = true;
            }

            _sweepTimer.Dispose();
            _cache.Clear();
        }
    }
}
