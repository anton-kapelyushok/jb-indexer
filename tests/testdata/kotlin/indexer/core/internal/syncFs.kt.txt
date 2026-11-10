package indexer.core.internal

import com.google.common.collect.Interner
import com.google.common.collect.Interners
import indexer.core.IndexConfig
import indexer.core.internal.FileEventSource.INITIAL_SYNC
import indexer.core.internal.FileEventSource.WATCHER
import indexer.core.internal.FileEventType.*
import io.methvin.watcher.DirectoryChangeEvent
import io.methvin.watcher.DirectoryChangeListener
import io.methvin.watcher.DirectoryWatcher
import io.methvin.watcher.hashing.FileHasher
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import org.slf4j.helpers.NOPLogger
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.exists
import kotlin.io.path.isRegularFile
import kotlin.streams.asSequence

internal suspend fun syncFs(
    cfg: IndexConfig,
    dir: Path,
    fileSyncEvents: SendChannel<FileSyncEvent>,
    statusUpdates: SendChannel<StatusUpdate>
) = coroutineScope {

    val clock = AtomicLong(0L)
    val faInterner = Interners.newWeakInterner<FileAddress>()

    while (true) {
        val watcherStartedLatch = CompletableDeferred<Unit>()
        val initialSyncCompleteLatch = CompletableDeferred<Unit>()

        val watcherDeferred = if (cfg.enableWatcher) {
            val deferred = async {
                watch(
                    dir,
                    clock,
                    faInterner,
                    fileSyncEvents,
                    statusUpdates,
                    initialSyncCompleteLatch,
                    watcherStartedLatch
                )
            }
            watcherStartedLatch.await()
            deferred
        } else {
            CompletableDeferred() // never completes
        }

        emitInitialContent(dir, cfg, clock, faInterner, initialSyncCompleteLatch, fileSyncEvents, statusUpdates)

        val watcherResult = watcherDeferred.await()
        val watcherException = watcherResult.exceptionOrNull()
            ?: IllegalStateException("Watcher completed without exception for some reason")

        cfg.handleWatcherError(watcherException)
        statusUpdates.send(StatusUpdate.FileSyncFailed(clock.incrementAndGet(), watcherException))
    }
}

internal suspend fun emitInitialContent(
    dir: Path,
    cfg: IndexConfig,
    clock: AtomicLong,
    faInterner: Interner<FileAddress>,
    initialSyncCompleteLatch: CompletableDeferred<Unit>,
    fileSyncEvents: SendChannel<FileSyncEvent>,
    statusUpdates: SendChannel<StatusUpdate>,
) {
    withContext(Dispatchers.IO) {
        val retryCount = 10
        for (attempt in 1..retryCount) {
            try {
                Files.walk(dir)
                    .asSequence()
                    .filter { it.isRegularFile() }
                    .forEach {
                        ensureActive()
                        statusUpdates.send(StatusUpdate.FileUpdated(INITIAL_SYNC))
                        fileSyncEvents.send(
                            FileSyncEvent(
                                clock.incrementAndGet(),
                                it.toFile().canonicalPath.toFileAddress(faInterner),
                                INITIAL_SYNC,
                                CREATE,
                            )
                        )
                    }
            } catch (e: Throwable) {
                coroutineContext.ensureActive()
                if (!dir.exists()) {
                    val e1 = FileNotFoundException(dir.toFile().canonicalPath)
                    e1.addSuppressed(e)
                    cfg.handleInitialFileSyncError(e1)
                    throw e
                }

                cfg.handleInitialFileSyncError(e)
                if (attempt == retryCount) throw e

                // the usual cause is someone is deleting directory content while we are trying to index it
                // there is a good chance that it will throw again if we retry immediately
                // add a backoff to handle this
                delay((attempt - 1) * 1000L)
                continue
            }
            break
        }

        initialSyncCompleteLatch.complete(Unit)
        statusUpdates.send(StatusUpdate.AllFilesDiscovered)
    }
}

internal suspend fun watch(
    dir: Path,
    clock: AtomicLong,
    faInterner: Interner<FileAddress>,
    fileSyncEvents: SendChannel<FileSyncEvent>,
    statusUpdates: SendChannel<StatusUpdate>,
    initialSyncCompleteLatch: CompletableDeferred<Unit>,
    watcherStartedLatch: CompletableDeferred<Unit>,
): Result<Any> {
    return runCatching {
        withContext(Dispatchers.IO) {
            val watcher = buildWatcher(
                coroutineContext,
                dir, clock, faInterner, initialSyncCompleteLatch, fileSyncEvents, statusUpdates
            )
            invokeOnCancellation { watcher.close() }

            // watcher.watchAsync() is reading the whole directory on start and is blocking
            // this operation takes about 20s on intellij-community repository - and we want to cancel it
            // funny thing is, this operation is not interruptible by default
            // this can be worked around by checking interruption flag in fileHasher,
            // which is called on every encountered file
            val future = runInterruptible { watcher.watchAsync() }
            statusUpdates.send(StatusUpdate.WatcherStarted)
            watcherStartedLatch.complete(Unit)
            future.join()
        }
    }
}

private fun buildWatcher(
    ctx: CoroutineContext,
    dir: Path,
    clock: AtomicLong,
    faInterner: Interner<FileAddress>,
    initialSyncCompleteLatch: CompletableDeferred<Unit>,
    fileSyncEvents: SendChannel<FileSyncEvent>,
    statusUpdates: SendChannel<StatusUpdate>,
): DirectoryWatcher = DirectoryWatcher.builder()
    .path(dir)
    .logger(NOPLogger.NOP_LOGGER)
    .fileHasher { path ->
        // A hack to fast cancel watcher.watchAsync()
        if (Thread.interrupted()) {
            throw InterruptedException()
        }

        // A hack to show some information during watcher initialization
        if (!initialSyncCompleteLatch.isCompleted) {
            runBlocking(ctx + Dispatchers.Unconfined) {
                statusUpdates.send(StatusUpdate.WatcherDiscoveredFileDuringInitialization)
            }
        }
        FileHasher.LAST_MODIFIED_TIME.hash(path)
    }
    .listener(object : DirectoryChangeListener {
        override fun onEvent(event: DirectoryChangeEvent) {
            if (event.isDirectory) return
            runBlocking(ctx + Dispatchers.Unconfined) {
                // buffer watcher events until all files are emitted
                initialSyncCompleteLatch.await()

                val t = clock.incrementAndGet()
                statusUpdates.send(StatusUpdate.FileUpdated(WATCHER))

                when (event.eventType()!!) {
                    DirectoryChangeEvent.EventType.CREATE -> {
                        fileSyncEvents.send(
                            FileSyncEvent(
                                t = t,
                                fileAddress = event.path().toFile().canonicalPath.toFileAddress(faInterner),
                                source = WATCHER,
                                type = CREATE
                            )
                        )
                    }

                    DirectoryChangeEvent.EventType.MODIFY -> {
                        fileSyncEvents.send(
                            FileSyncEvent(
                                t = t,
                                fileAddress = event.path().toFile().canonicalPath.toFileAddress(faInterner),
                                source = WATCHER,
                                type = MODIFY
                            )
                        )
                    }

                    DirectoryChangeEvent.EventType.DELETE -> {
                        fileSyncEvents.send(
                            FileSyncEvent(
                                t = t,
                                fileAddress = event.path().toFile().canonicalPath.toFileAddress(faInterner),
                                source = WATCHER,
                                type = DELETE
                            )
                        )
                    }

                    DirectoryChangeEvent.EventType.OVERFLOW -> throw WatcherOverflowException()
                }
            }
        }

        override fun onException(e: Exception) {
            throw e
        }
    })
    .build()

internal class WatcherOverflowException : RuntimeException()

private fun String.toFileAddress(interner: Interner<FileAddress>) = interner.intern(FileAddress(this))