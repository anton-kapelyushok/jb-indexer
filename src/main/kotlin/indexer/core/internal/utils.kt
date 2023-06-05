package indexer.core.internal

import indexer.core.IndexConfig
import kotlinx.coroutines.*

/**
 * Invokes `close` on coroutine cancellation
 * Allows using closeable blocking resources in coroutine context.
 *
 * Example:
 *
 * withContext(Dispatchers.IO) {
 *     val watcher = DirectoryWatcher()
 *     invokeOnCancellation { watcher.close() }
 *     watcher.watch()
 * }
 *
 * watcher.watch() is blocking, the only way to terminate it is by calling watcher.close()
 * invokeOnCancellation will call watcher.close() on coroutine cancellation
 *
 */
internal suspend fun CoroutineScope.invokeOnCancellation(close: suspend () -> Unit) {
    val parentScope = this
    val closerStartedLatch = CompletableDeferred<Unit>()

    parentScope.launch {
        try {
            closerStartedLatch.complete(Unit)
            awaitCancellation()
        } finally {
            withContext(NonCancellable) {
                close()
            }
        }
    }
    closerStartedLatch.await()
}

internal fun IndexConfig.debugLog(str: String) {
    if (enableLogging.get()) println(str)
}