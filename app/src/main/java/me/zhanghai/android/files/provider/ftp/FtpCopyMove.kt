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
import me.zhanghai.android.files.provider.common.createDirectories
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
        return source.toString().startsWith("ftp://") && target.toString().startsWith("ftp://")
    }

    /**
     * Copy a file with optimized FTP implementation
     */
    @Throws(IOException::class)
    fun copy(source: Path, target: Path, copyOptions: CopyOptions): Boolean {
        // Get source file size to determine buffer size
        val sourceSize = try {
            (source as? Java8Path)?.fileSystem?.provider()?.readAttributes(source, "basic:size")?.get("size") as? Long
        } catch (e: Exception) {
            null
        } ?: DEFAULT_BUFFER_SIZE.toLong()
        
        // Choose buffer size based on file size
        val bufferSize = when {
            sourceSize > LARGE_BUFFER_SIZE -> LARGE_BUFFER_SIZE
            else -> DEFAULT_BUFFER_SIZE
        }

        return try {
            // Create parent directories if needed
            target.parent?.let {
                (it as? Java8Path)?.fileSystem?.provider()?.createDirectory(it)
            }
            
            // Stream the file from source to target
            (source as? Java8Path)?.fileSystem?.provider()?.newInputStream(source).use { input ->
                (target as? Java8Path)?.fileSystem?.provider()?.newOutputStream(target).use { output ->
                    val buffer = ByteArray(bufferSize)
                    var totalBytesRead = 0L
                    var lastProgressUpdateBytes = 0L
                    var bytesRead: Int

                    while (input?.read(buffer).also { bytesRead = it ?: -1 } != -1) {
                        output?.write(buffer, 0, bytesRead)
                        totalBytesRead += bytesRead

                        // Report progress periodically
                        if (copyOptions.progressListener != null &&
                            totalBytesRead - lastProgressUpdateBytes >= bufferSize) {
                            copyOptions.progressListener.invoke(totalBytesRead - lastProgressUpdateBytes)
                            lastProgressUpdateBytes = totalBytesRead
                        }
                    }

                    // Report final progress if any bytes remain
                    if (copyOptions.progressListener != null && totalBytesRead > lastProgressUpdateBytes) {
                        copyOptions.progressListener.invoke(totalBytesRead - lastProgressUpdateBytes)
                    }
                }
            }
            true
        } catch (e: IOException) {
            false
        }
    }

    /**
     * Move a file with optimized FTP implementation if possible, or fall back to copy+delete
     */
    @Throws(IOException::class)
    fun move(source: Path, target: Path, copyOptions: CopyOptions): Boolean {
        // Check if source and target are on the same FTP server
        val sameServer = try {
            val sourceAuthority = (source as? FtpPath)?.authority
            val targetAuthority = (target as? FtpPath)?.authority
            sourceAuthority == targetAuthority
        } catch (e: Exception) {
            false
        }

        return if (sameServer) {
            try {
                Client.renameFile(source, target)
                true
            } catch (e: IOException) {
                // Fall back to copy + delete if rename fails
                val copied = copy(source, target, copyOptions)
                if (copied) {
                    try {
                        Client.delete(source, false)
                    } catch (e: IOException) {
                        e.printStackTrace()
                    }
                }
                copied
            }
        } else {
            // Different servers, need to copy + delete
            val copied = copy(source, target, copyOptions)
            if (copied) {
                try {
                    Client.delete(source, false)
                } catch (e: IOException) {
                    e.printStackTrace()
                }
            }
            copied
        }
    }
}
