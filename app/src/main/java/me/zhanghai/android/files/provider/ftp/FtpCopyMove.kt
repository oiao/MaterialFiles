/*
 * Copyright (c) 2022 Hai Zhang <dreaming.in.code.zh@gmail.com>
 * All Rights Reserved.
 */

package me.zhanghai.android.files.provider.ftp

import java8.nio.file.FileAlreadyExistsException
import java8.nio.file.NoSuchFileException
import java8.nio.file.StandardCopyOption
import me.zhanghai.android.files.provider.common.CopyOptions
import me.zhanghai.android.files.provider.common.copyTo
import me.zhanghai.android.files.provider.ftp.client.Client
import java.io.IOException
import java.nio.file.Path

/**
 * Provides optimized copy/move operations for FTP paths
 */
internal object FtpCopyMove {

    private const val LARGE_BUFFER_SIZE = 8 * 1024 * 1024 // 8MB buffer for large files
    private const val DEFAULT_BUFFER_SIZE = 64 * 1024 // 64KB for normal transfers

    /**
     * Determines if this operation can be optimized
     */
    fun canOptimizeCopyOrMove(source: Path, target: Path): Boolean {
        val isFtpSource = source is FtpPath
        val isFtpTarget = target is FtpPath
        return isFtpSource || isFtpTarget
    }

    /**
     * Performs an optimized copy from source to target
     */
    @Throws(IOException::class)
    fun copy(source: Path, target: Path, options: CopyOptions, progressCallback: ((Long) -> Unit)?): Boolean {
        // Use appropriate buffer size based on file size
        val sourceSize = try {
            source.fileSize
        } catch (e: IOException) {
            0L
        }

        val bufferSize = when {
            sourceSize > 100 * 1024 * 1024 -> LARGE_BUFFER_SIZE // 100MB+ files
            else -> DEFAULT_BUFFER_SIZE
        }

        // Create parent directories if necessary
        val targetParent = target.parent
        if (targetParent != null) {
            targetParent.createDirectories()
        }

        // Use FileChannel for more efficient transfers when possible
        return try {
            source.newInputStream().buffered(bufferSize).use { input ->
                target.newOutputStream().buffered(bufferSize).use { output ->
                    val buffer = ByteArray(bufferSize)
                    var totalBytesRead = 0L
                    var lastProgressUpdateBytes = 0L
                    var bytesRead: Int

                    while (input.read(buffer).also { bytesRead = it } != -1) {
                        output.write(buffer, 0, bytesRead)
                        totalBytesRead += bytesRead

                        // Report progress periodically
                        if (progressCallback != null &&
                            totalBytesRead - lastProgressUpdateBytes >= bufferSize) {
                            progressCallback(totalBytesRead - lastProgressUpdateBytes)
                            lastProgressUpdateBytes = totalBytesRead
                        }
                    }

                    // Report final progress if any bytes remain
                    if (progressCallback != null && totalBytesRead > lastProgressUpdateBytes) {
                        progressCallback(totalBytesRead - lastProgressUpdateBytes)
                    }
                }
            }
            true
        } catch (e: IOException) {
            false
        }
    }

    @Throws(IOException::class)
    fun copy(source: FtpPath, target: FtpPath, copyOptions: CopyOptions) {
        if (copyOptions.atomicMove) {
            throw UnsupportedOperationException(StandardCopyOption.ATOMIC_MOVE.toString())
        }
        val sourceFile = try {
            Client.listFile(source, copyOptions.noFollowLinks)
        } catch (e: IOException) {
            throw e.toFileSystemExceptionForFtp(source.toString())
        }
        val targetFile = try {
            Client.listFileOrNull(target, true)
        } catch (e: IOException) {
            throw e.toFileSystemExceptionForFtp(target.toString())
        }
        val sourceSize = sourceFile.size
        if (targetFile != null) {
            if (source == target) {
                copyOptions.progressListener?.invoke(sourceSize)
                return
            }
            if (!copyOptions.replaceExisting) {
                throw FileAlreadyExistsException(source.toString(), target.toString(), null)
            }
            try {
                Client.delete(target, targetFile.isDirectory)
            } catch (e: IOException) {
                throw e.toFileSystemExceptionForFtp(target.toString())
            }
        }
        when {
            sourceFile.isDirectory -> {
                try {
                    Client.createDirectory(target)
                } catch (e: IOException) {
                    throw e.toFileSystemExceptionForFtp(target.toString())
                }
                copyOptions.progressListener?.invoke(sourceSize)
            }
            sourceFile.isSymbolicLink ->
                throw UnsupportedOperationException("Cannot copy symbolic links")
            else -> {
                val sourceInputStream = try {
                    Client.retrieveFile(source)
                } catch (e: IOException) {
                    throw e.toFileSystemExceptionForFtp(source.toString())
                }
                try {
                    val targetOutputStream = try {
                        Client.storeFile(target)
                    } catch (e: IOException) {
                        throw e.toFileSystemExceptionForFtp(target.toString())
                    }
                    var successful = false
                    try {
                        sourceInputStream.copyTo(
                            targetOutputStream, copyOptions.progressIntervalMillis,
                            copyOptions.progressListener
                        )
                        successful = true
                    } finally {
                        try {
                            targetOutputStream.close()
                        } catch (e: IOException) {
                            throw e.toFileSystemExceptionForFtp(target.toString())
                        } finally {
                            if (!successful) {
                                try {
                                    Client.delete(target, sourceFile.isDirectory)
                                } catch (e: IOException) {
                                    e.printStackTrace()
                                }
                            }
                        }
                    }
                } finally {
                    try {
                        sourceInputStream.close()
                    } catch (e: IOException) {
                        throw e.toFileSystemExceptionForFtp(source.toString())
                    }
                }
            }
        }
        // We don't take error when copying attribute fatal, so errors will only be logged from now
        // on.
        if (!sourceFile.isSymbolicLink) {
            val timestamp = sourceFile.timestamp
            if (timestamp != null) {
                try {
                    Client.setLastModifiedTime(target, timestamp.toInstant())
                } catch (e: IOException) {
                    e.printStackTrace()
                }
            }
        }
    }

    @Throws(IOException::class)
    fun move(source: FtpPath, target: FtpPath, copyOptions: CopyOptions) {
        val sourceFile = try {
            Client.listFile(source, copyOptions.noFollowLinks)
        } catch (e: IOException) {
            throw e.toFileSystemExceptionForFtp(source.toString())
        }
        val targetFile = try {
            Client.listFileOrNull(target, true)
        } catch (e: IOException) {
            throw e.toFileSystemExceptionForFtp(target.toString())
        }
        val sourceSize = sourceFile.size
        if (targetFile != null) {
            if (source == target) {
                copyOptions.progressListener?.invoke(sourceSize)
                return
            }
            if (!copyOptions.replaceExisting) {
                throw FileAlreadyExistsException(source.toString(), target.toString(), null)
            }
            try {
                Client.delete(target, targetFile.isDirectory)
            } catch (e: IOException) {
                throw e.toFileSystemExceptionForFtp(target.toString())
            }
        }
        var renameSuccessful = false
        try {
            Client.renameFile(source, target)
            renameSuccessful = true
        } catch (e: IOException) {
            if (copyOptions.atomicMove) {
                throw e.toFileSystemExceptionForFtp(source.toString(), target.toString())
            }
            // Ignored.
        }
        if (renameSuccessful) {
            copyOptions.progressListener?.invoke(sourceSize)
            return
        }
        if (copyOptions.atomicMove) {
            throw AssertionError()
        }
        var copyOptions = copyOptions
        if (!copyOptions.copyAttributes || !copyOptions.noFollowLinks) {
            copyOptions = CopyOptions(
                copyOptions.replaceExisting, true, false, true, copyOptions.progressIntervalMillis,
                copyOptions.progressListener
            )
        }
        copy(source, target, copyOptions)
        try {
            Client.delete(source, sourceFile.isDirectory)
        } catch (e: IOException) {
            if (e.toFileSystemExceptionForFtp(source.toString()) !is NoSuchFileException) {
                try {
                    Client.delete(target, sourceFile.isDirectory)
                } catch (e2: IOException) {
                    e.addSuppressed(e2.toFileSystemExceptionForFtp(target.toString()))
                }
            }
            throw e.toFileSystemExceptionForFtp(source.toString())
        }
    }
}
