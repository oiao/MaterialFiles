/*
 * Copyright (c) 2019 Hai Zhang <dreaming.in.code.zh@gmail.com>
 * All Rights Reserved.
 */

package me.zhanghai.android.files.filejob

import android.app.PendingIntent
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Environment
import android.widget.Toast
import androidx.annotation.AnyRes
import androidx.annotation.PluralsRes
import androidx.annotation.StringRes
import java8.nio.file.CopyOption
import java8.nio.file.DirectoryIteratorException
import java8.nio.file.FileAlreadyExistsException
import java8.nio.file.FileVisitResult
import java8.nio.file.FileVisitor
import java8.nio.file.Files
import java8.nio.file.LinkOption
import java8.nio.file.Path
import java8.nio.file.Paths
import java8.nio.file.SimpleFileVisitor
import java8.nio.file.StandardCopyOption
import java8.nio.file.StandardOpenOption
import java8.nio.file.attribute.BasicFileAttributes
import kotlinx.coroutines.runBlocking
import me.zhanghai.android.files.R
import me.zhanghai.android.files.app.BackgroundActivityStarter
import me.zhanghai.android.files.app.mainExecutor
import me.zhanghai.android.files.compat.mainExecutorCompat
import me.zhanghai.android.files.file.FileItem
import me.zhanghai.android.files.file.MimeType
import me.zhanghai.android.files.file.asFileSize
import me.zhanghai.android.files.file.fileProviderUri
import me.zhanghai.android.files.file.loadFileItem
import me.zhanghai.android.files.filelist.OpenFileAsDialogActivity
import me.zhanghai.android.files.filelist.OpenFileAsDialogFragment
import me.zhanghai.android.files.provider.archive.archiveFile
import me.zhanghai.android.files.provider.archive.archiver.ArchiveWriter
import me.zhanghai.android.files.provider.archive.createArchiveRootPath
import me.zhanghai.android.files.provider.archive.isArchivePath
import me.zhanghai.android.files.provider.common.ByteString
import me.zhanghai.android.files.provider.common.ByteStringBuilder
import me.zhanghai.android.files.provider.common.InvalidFileNameException
import me.zhanghai.android.files.provider.common.PosixFileModeBit
import me.zhanghai.android.files.provider.common.PosixFileStore
import me.zhanghai.android.files.provider.common.PosixGroup
import me.zhanghai.android.files.provider.common.PosixPrincipal
import me.zhanghai.android.files.provider.common.PosixUser
import me.zhanghai.android.files.provider.common.ProgressCopyOption
import me.zhanghai.android.files.provider.common.ReadOnlyFileSystemException
import me.zhanghai.android.files.provider.common.UserActionRequiredException
import me.zhanghai.android.files.provider.common.asByteStringListPath
import me.zhanghai.android.files.provider.common.copyTo
import me.zhanghai.android.files.provider.common.createDirectories
import me.zhanghai.android.files.provider.common.createDirectory
import me.zhanghai.android.files.provider.common.createFile
import me.zhanghai.android.files.provider.common.delete
import me.zhanghai.android.files.provider.common.deleteIfExists
import me.zhanghai.android.files.provider.common.exists
import me.zhanghai.android.files.provider.common.getFileStore
import me.zhanghai.android.files.provider.common.getMode
import me.zhanghai.android.files.provider.common.getPath
import me.zhanghai.android.files.provider.common.isDirectory
import me.zhanghai.android.files.provider.common.moveTo
import me.zhanghai.android.files.provider.common.newByteChannel
import me.zhanghai.android.files.provider.common.newDirectoryStream
import me.zhanghai.android.files.provider.common.newOutputStream
import me.zhanghai.android.files.provider.common.readAttributes
import me.zhanghai.android.files.provider.common.resolveForeign
import me.zhanghai.android.files.provider.common.restoreSeLinuxContext
import me.zhanghai.android.files.provider.common.setGroup
import me.zhanghai.android.files.provider.common.setMode
import me.zhanghai.android.files.provider.common.setOwner
import me.zhanghai.android.files.provider.common.setSeLinuxContext
import me.zhanghai.android.files.provider.common.toByteString
import me.zhanghai.android.files.provider.common.toModeString
import me.zhanghai.android.files.provider.linux.isLinuxPath
import me.zhanghai.android.files.util.asFileName
import me.zhanghai.android.files.util.createInstallPackageIntent
import me.zhanghai.android.files.util.createIntent
import me.zhanghai.android.files.util.createViewIntent
import me.zhanghai.android.files.util.extraPath
import me.zhanghai.android.files.util.getQuantityString
import me.zhanghai.android.files.util.putArgs
import me.zhanghai.android.files.util.showToast
import me.zhanghai.android.files.util.toEnumSet
import me.zhanghai.android.files.util.withChooser
import java.io.ByteArrayInputStream
import java.io.File
import java.io.IOException
import java.io.InterruptedIOException
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

fun FileJob.getString(@StringRes stringRes: Int): String {
    return service.getString(stringRes)
}

fun FileJob.getString(@StringRes stringRes: Int, vararg formatArguments: Any?): String {
    return service.getString(stringRes, *formatArguments)
}

fun FileJob.getQuantityString(@PluralsRes pluralRes: Int, quantity: Int): String {
    return service.getQuantityString(pluralRes, quantity)
}

fun FileJob.getQuantityString(
    @PluralsRes pluralRes: Int,
    quantity: Int,
    vararg formatArguments: Any?
): String {
    return service.getQuantityString(pluralRes, quantity, *formatArguments)
}

private fun FileJob.postNotification(
    title: CharSequence,
    text: CharSequence?,
    subText: CharSequence?,
    info: CharSequence?,
    max: Int,
    progress: Int,
    indeterminate: Boolean,
    showCancel: Boolean
) {
    val notification = fileJobNotificationTemplate.createBuilder(service).apply {
        setContentTitle(title)
        setContentText(text)
        setSubText(subText)
        setContentInfo(info)
        setProgress(max, progress, indeterminate)
        // TODO
        //setContentIntent()
        if (showCancel) {
            val intent = FileJobReceiver.createIntent(id)
            var pendingIntentFlags = PendingIntent.FLAG_UPDATE_CURRENT
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                pendingIntentFlags = pendingIntentFlags or PendingIntent.FLAG_IMMUTABLE
            }
            val pendingIntent = PendingIntent.getBroadcast(
                service, id + 1, intent, pendingIntentFlags
            )
            addAction(
                R.drawable.close_icon_white_24dp, getString(android.R.string.cancel), pendingIntent
            )
        }
    }.build()
    service.notificationManager.notify(id, notification)
}

// Increase the progress interval to reduce overhead during transfers
private const val PROGRESS_INTERVAL_MILLIS = 500L  // Changed from 150ms to 500ms for better performance
private const val NOTIFICATION_INTERVAL_MILLIS = 500L

private fun FileJob.showToast(textRes: Int, duration: Int = Toast.LENGTH_SHORT) {
    service.mainExecutorCompat.execute {
        service.showToast(textRes, duration)
    }
}

private fun FileJob.showToast(text: CharSequence, duration: Int = Toast.LENGTH_SHORT) {
    service.mainExecutorCompat.execute {
        service.showToast(text, duration)
    }
}

private fun FileJob.getFileName(path: Path): String =
    if (path.isAbsolute && path.nameCount == 0) {
        path.fileSystem.separator
    } else {
        path.fileName.toString()
    }

private fun FileJob.getTargetFileName(source: Path): Path {
    if (source.isArchivePath) {
        val archiveFile = source.archiveFile.asByteStringListPath()
        val archiveRoot = archiveFile.createArchiveRootPath()
        if (source == archiveRoot) {
            return archiveFile.fileSystem.getPath(
                archiveFile.fileNameByteString!!.asFileName().baseName
            )
        }
    }
    return source.fileName
}

// The attributes for start path prefers following links, but falls back to not following.
// FileVisitResult returned from visitor may be ignored and always considered CONTINUE.
@Throws(IOException::class)
private fun FileJob.walkFileTreeForSettingAttributes(
    start: Path,
    recursive: Boolean,
    visitor: FileVisitor<in Path>
): Path {
    val attributes = try {
        start.readAttributes(BasicFileAttributes::class.java)
    } catch (ignored: IOException) {
        try {
            start.readAttributes(BasicFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS)
        } catch (e: IOException) {
            visitor.visitFileFailed(start, e)
            return start
        }
    }
    if (!recursive || !attributes.isDirectory) {
        visitor.visitFile(start, attributes)
        return start
    }
    val directoryStream = try {
        start.newDirectoryStream()
    } catch (e: IOException) {
        visitor.visitFileFailed(start, e)
        return start
    }
    directoryStream.use {
        visitor.preVisitDirectory(start, attributes)
        try {
            directoryStream.forEach { Files.walkFileTree(it, visitor) }
        } catch (e: DirectoryIteratorException) {
            visitor.postVisitDirectory(start, e.cause)
            return start
        }
    }
    visitor.postVisitDirectory(start, null)
    return start
}

@Throws(InterruptedIOException::class)
private fun FileJob.throwIfInterrupted() {
    if (Thread.interrupted()) {
        throw InterruptedIOException()
    }
}

@Throws(IOException::class)
private fun FileJob.scan(sources: List<Path?>, @PluralsRes notificationTitleRes: Int): ScanInfo {
    val scanInfo = ScanInfo()
    for (source in sources) {
        Files.walkFileTree(source, object : SimpleFileVisitor<Path>() {
            @Throws(IOException::class)
            override fun preVisitDirectory(
                directory: Path,
                attributes: BasicFileAttributes
            ): FileVisitResult {
                scanPath(attributes, scanInfo, notificationTitleRes)
                throwIfInterrupted()
                return FileVisitResult.CONTINUE
            }

            @Throws(IOException::class)
            override fun visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult {
                scanPath(attributes, scanInfo, notificationTitleRes)
                throwIfInterrupted()
                return FileVisitResult.CONTINUE
            }

            @Throws(IOException::class)
            override fun visitFileFailed(file: Path, exception: IOException): FileVisitResult {
                // TODO: Prompt retry, skip, skip-all or abort.
                return super.visitFileFailed(file, exception)
            }
        })
    }
    postScanNotification(scanInfo, notificationTitleRes)
    return scanInfo
}

@Throws(IOException::class)
private fun FileJob.scan(source: Path, @PluralsRes notificationTitleRes: Int): ScanInfo {
    return scan(listOf(source), notificationTitleRes)
}

@Throws(IOException::class)
private fun FileJob.scan(
    source: Path,
    recursive: Boolean,
    @PluralsRes notificationTitleRes: Int
): ScanInfo {
    if (recursive) {
        return scan(source, notificationTitleRes)
    }
    val scanInfo = ScanInfo()
    val attributes = source.readAttributes(
        BasicFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS
    )
    scanPath(attributes, scanInfo, notificationTitleRes)
    throwIfInterrupted()
    return scanInfo
}

private fun FileJob.scanPath(
    attributes: BasicFileAttributes,
    scanInfo: ScanInfo,
    @PluralsRes notificationTitleRes: Int
) {
    scanInfo.incrementFileCount()
    scanInfo.addToSize(attributes.size())
    postScanNotification(scanInfo, notificationTitleRes)
}

private fun FileJob.postScanNotification(scanInfo: ScanInfo, @PluralsRes titleRes: Int) {
    if (!scanInfo.shouldPostNotification()) {
        return
    }
    val size = scanInfo.size.asFileSize().formatHumanReadable(service)
    val fileCount: Int = scanInfo.fileCount
    val title: String = getQuantityString(titleRes, fileCount, fileCount, size)
    postNotification(title, null, null, null, 0, 0, true, true)
}

private class ScanInfo {
    var fileCount = 0
        private set
    var size = 0L
        private set

    private var lastNotificationTimeMillis = 0L

    fun incrementFileCount() {
        ++fileCount
    }

    fun addToSize(size: Long) {
        this.size += size
    }

    fun shouldPostNotification(): Boolean {
        val currentTimeMillis = System.currentTimeMillis()
        return if (fileCount % 100 == 0
            || lastNotificationTimeMillis + NOTIFICATION_INTERVAL_MILLIS < currentTimeMillis) {
            lastNotificationTimeMillis = currentTimeMillis
            true
        } else {
            false
        }
    }
}

private fun FileJob.postTransferSizeNotification(
    transferInfo: TransferInfo,
    currentSource: Path,
    @StringRes titleOneRes: Int,
    @PluralsRes titleMultipleRes: Int
) {
    if (!transferInfo.shouldPostNotification()) {
        return
    }
    val title: String
    val text: String
    val fileCount = transferInfo.fileCount
    val target = transferInfo.target!!
    val size = transferInfo.size
    val transferredSize = transferInfo.transferredSize
    if (fileCount == 1) {
        title = getString(titleOneRes, getFileName(currentSource), getFileName(target))
        val sizeString = size.asFileSize().formatHumanReadable(service)
        val transferredSizeString = transferredSize.asFileSize().formatHumanReadable(service)
        text = getString(
            R.string.file_job_transfer_size_notification_text_one_format, transferredSizeString,
            sizeString
        )
    } else {
        title = getQuantityString(titleMultipleRes, fileCount, fileCount, getFileName(target))
        val currentFileIndex = (transferInfo.transferredFileCount + 1)
            .coerceAtMost(fileCount)
        text = getString(
            R.string.file_job_transfer_size_notification_text_multiple_format, currentFileIndex,
            fileCount
        )
    }
    val max: Int
    val progress: Int
    if (size <= Int.MAX_VALUE) {
        max = size.toInt()
        progress = transferredSize.toInt()
    } else {
        var maxLong = size
        var progressLong = transferredSize
        while (maxLong > Int.MAX_VALUE) {
            maxLong /= 2
            progressLong /= 2
        }
        max = maxLong.toInt()
        progress = progressLong.toInt()
    }
    postNotification(title, text, null, null, max, progress, false, true)
}

private fun FileJob.postTransferCountNotification(
    transferInfo: TransferInfo,
    currentPath: Path,
    @StringRes titleOneRes: Int,
    @PluralsRes titleMultipleRes: Int
) {
    if (!transferInfo.shouldPostNotification()) {
        return
    }
    val title: String
    val text: String?
    val max: Int
    val progress: Int
    val indeterminate: Boolean
    val fileCount = transferInfo.fileCount
    if (fileCount == 1) {
        title = getString(titleOneRes, getFileName(currentPath))
        text = null
        max = 0
        progress = 0
        indeterminate = true
    } else {
        title = getQuantityString(titleMultipleRes, fileCount, fileCount)
        val transferredFileCount = transferInfo.transferredFileCount
        val currentFileIndex = (transferredFileCount + 1).coerceAtMost(fileCount)
        text = getString(
            R.string.file_job_transfer_count_notification_text_multiple_format, currentFileIndex,
            fileCount
        )
        max = fileCount
        progress = transferredFileCount
        indeterminate = false
    }
    postNotification(title, text, null, null, max, progress, indeterminate, true)
}

private class TransferInfo(scanInfo: ScanInfo, val target: Path?) {
    var fileCount: Int = scanInfo.fileCount
        private set
    var transferredFileCount = 0
        private set
    var size: Long = scanInfo.size
        private set
    var transferredSize = 0L
        private set

    private var lastNotificationTimeMillis = 0L

    fun incrementTransferredFileCount() {
        ++transferredFileCount
    }

    fun addTransferredFile(path: Path) {
        ++transferredFileCount
        try {
            transferredSize += path.readAttributes(
                BasicFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS
            ).size()
        } catch (e: IOException) {
            e.printStackTrace()
        }
    }

    fun skipFile(path: Path) {
        --fileCount
        try {
            size -= path.readAttributes(
                BasicFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS
            ).size()
        } catch (e: IOException) {
            e.printStackTrace()
        }
    }

    fun skipFileIgnoringSize() {
        --fileCount
    }

    fun addToTransferredSize(size: Long) {
        transferredSize += size
    }

    fun shouldPostNotification(): Boolean {
        val currentTimeMillis = System.currentTimeMillis()
        return if (lastNotificationTimeMillis + NOTIFICATION_INTERVAL_MILLIS < currentTimeMillis) {
            lastNotificationTimeMillis = currentTimeMillis
            true
        } else {
            false
        }
    }
}

private fun FileJob.postRestoreSeLinuxContextNotification(transferInfo: TransferInfo, currentPath: Path) {
    postTransferCountNotification(
        transferInfo, currentPath,
        R.string.file_job_restore_selinux_context_notification_title_one_format,
        R.plurals.file_job_restore_selinux_context_notification_title_multiple_format
    )
}

class SaveFileJob(private val source: Path, private val target: Path) : FileJob() {
    @Throws(IOException::class)
    override fun run() {
        val scanInfo = scan(source, false, R.plurals.file_job_copy_scan_notification_title_format)
        val transferInfo = TransferInfo(scanInfo, target.parent)
        val actionAllInfo = ActionAllInfo()
        copyRecursively(source, target, false, transferInfo, actionAllInfo)
    }
}

class SetFileGroupJob(
    private val path: Path,
    private val group: PosixGroup,
    private val recursive: Boolean
) : FileJob() {
    @Throws(IOException::class)
    override fun run() {
        val scanInfo = scan(
            path, recursive,
            R.plurals.file_job_set_group_scan_notification_title_format
        )
        val transferInfo = TransferInfo(scanInfo, null)
        val actionAllInfo = ActionAllInfo()
        walkFileTreeForSettingAttributes(path, recursive, object : SimpleFileVisitor<Path>() {
            @Throws(IOException::class)
            override fun preVisitDirectory(
                directory: Path,
                attributes: BasicFileAttributes
            ): FileVisitResult = visitFile(directory, attributes)

            @Throws(IOException::class)
            override fun visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult {
                setGroup(file, transferInfo, actionAllInfo)
                throwIfInterrupted()
                return FileVisitResult.CONTINUE
            }

            @Throws(IOException::class)
            override fun visitFileFailed(file: Path, exception: IOException): FileVisitResult {
                return super.visitFileFailed(file, exception)
            }
        })
    }

    @Throws(IOException::class)
    private fun setGroup(path: Path, transferInfo: TransferInfo, actionAllInfo: ActionAllInfo) {
        var retry: Boolean
        do {
            retry = false
            try {
                path.setGroup(group)
                transferInfo.incrementTransferredFileCount()
                postSetGroupNotification(transferInfo, path)
            } catch (e: InterruptedIOException) {
                throw e
            } catch (e: IOException) {
                e.printStackTrace()
                if (actionAllInfo.skipSetGroupError) {
                    transferInfo.skipFileIgnoringSize()
                    postSetGroupNotification(transferInfo, path)
                    return
                }
                if (e is UserActionRequiredException) {
                    val result = showUserAction(e)
                    if (result) {
                        retry = true
                        continue
                    }
                }
                val result = showErrorDialog(
                    getString(R.string.file_job_set_group_error_title),
                    getString(
                        R.string.file_job_set_group_error_message_format, getFileName(path),
                        e.toString()
                    ),
                    getReadOnlyFileStore(path, e),
                    true,
                    getString(R.string.retry),
                    getString(R.string.skip),
                    getString(android.R.string.cancel)
                )
                when (result.action) {
                    FileJobErrorAction.POSITIVE -> {
                        retry = true
                        continue
                    }
                    FileJobErrorAction.NEGATIVE -> {
                        if (result.isAll) {
                            actionAllInfo.skipSetGroupError = true
                        }
                        transferInfo.skipFileIgnoringSize()
                        postSetGroupNotification(transferInfo, path)
                        return
                    }
                    FileJobErrorAction.CANCELED -> {
                        transferInfo.skipFileIgnoringSize()
                        postSetGroupNotification(transferInfo, path)
                        return
                    }
                    FileJobErrorAction.NEUTRAL -> throw InterruptedIOException()
                }
            }
        } while (retry)
    }

    private fun postSetGroupNotification(transferInfo: TransferInfo, currentPath: Path) {
        postTransferCountNotification(
            transferInfo, currentPath, R.string.file_job_set_group_notification_title_one_format,
            R.plurals.file_job_set_group_notification_title_multiple_format
        )
    }
}

class SetFileModeJob(
    private val path: Path,
    private val mode: Set<PosixFileModeBit>,
    private val recursive: Boolean,
    private val uppercaseX: Boolean
) : FileJob() {
    @Throws(IOException::class)
    override fun run() {
        val scanInfo = scan(
            path, recursive,
            R.plurals.file_job_set_mode_scan_notification_title_format
        )
        val transferInfo = TransferInfo(scanInfo, null)
        val actionAllInfo = ActionAllInfo()
        val title = if (uppercaseX) {
            getString(
                R.string.file_job_set_mode_notification_title_one_format, getFileName(path),
                mode.toModeString(true)
            )
        } else {
            getString(
                R.string.file_job_set_mode_notification_title_one_format, getFileName(path),
                mode.toModeString()
            )
        }
        postNotification(title, null, null, null, 0, 0, true, true)
        walkFileTreeForSettingAttributes(path, recursive, object : SimpleFileVisitor<Path>() {
            @Throws(IOException::class)
            override fun preVisitDirectory(
                directory: Path,
                attributes: BasicFileAttributes
            ): FileVisitResult = visitFile(directory, attributes)

            @Throws(IOException::class)
            override fun visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult {
                setMode(file, attributes, transferInfo, actionAllInfo)
                throwIfInterrupted()
                return FileVisitResult.CONTINUE
            }

            @Throws(IOException::class)
            override fun visitFileFailed(file: Path, exception: IOException): FileVisitResult {
                return super.visitFileFailed(file, exception)
            }
        })
    }

    @Throws(IOException::class)
    private fun setMode(
        path: Path,
        attributes: BasicFileAttributes,
        transferInfo: TransferInfo,
        actionAllInfo: ActionAllInfo
    ) {
        var retry: Boolean
        do {
            retry = false
            try {
                val mode = if (uppercaseX) {
                    val finalMode = mode.toMutableSet()
                    path.getMode()?.let { oldMode ->
                        if (oldMode.contains(PosixFileModeBit.OWNER_EXECUTE)) {
                            finalMode += PosixFileModeBit.OWNER_EXECUTE
                        }
                        if (oldMode.contains(PosixFileModeBit.GROUP_EXECUTE)) {
                            finalMode += PosixFileModeBit.GROUP_EXECUTE
                        }
                        if (oldMode.contains(PosixFileModeBit.OTHERS_EXECUTE)) {
                            finalMode += PosixFileModeBit.OTHERS_EXECUTE
                        }
                    } ?: mode
                    finalMode
                } else {
                    mode
                }
                path.setMode(mode)
                transferInfo.incrementTransferredFileCount()
                postSetModeNotification(transferInfo, path)
            } catch (e: InterruptedIOException) {
                throw e
            } catch (e: IOException) {
                e.printStackTrace()
                if (actionAllInfo.skipSetModeError) {
                    transferInfo.skipFileIgnoringSize()
                    postSetModeNotification(transferInfo, path)
                    return
                }
                if (e is UserActionRequiredException) {
                    val result = showUserAction(e)
                    if (result) {
                        retry = true
                        continue
                    }
                }
                val result = showErrorDialog(
                    getString(R.string.file_job_set_mode_error_title),
                    getString(
                        R.string.file_job_set_mode_error_message_format, getFileName(path),
                        e.toString()
                    ),
                    getReadOnlyFileStore(path, e),
                    true,
                    getString(R.string.retry),
                    getString(R.string.skip),
                    getString(android.R.string.cancel)
                )
                when (result.action) {
                    FileJobErrorAction.POSITIVE -> {
                        retry = true
                        continue
                    }
                    FileJobErrorAction.NEGATIVE -> {
                        if (result.isAll) {
                            actionAllInfo.skipSetModeError = true
                        }
                        transferInfo.skipFileIgnoringSize()
                        postSetModeNotification(transferInfo, path)
                        return
                    }
                    FileJobErrorAction.CANCELED -> {
                        transferInfo.skipFileIgnoringSize()
                        postSetModeNotification(transferInfo, path)
                        return
                    }
                    FileJobErrorAction.NEUTRAL -> throw InterruptedIOException()
                }
            }
        } while (retry)
    }

    private fun postSetModeNotification(transferInfo: TransferInfo, currentPath: Path) {
        postTransferCountNotification(
            transferInfo, currentPath, R.string.file_job_set_mode_notification_title_one_format,
            R.plurals.file_job_set_mode_notification_title_multiple_format
        )
    }
}

class SetFileOwnerJob(
    private val path: Path,
    private val owner: PosixUser,
    private val recursive: Boolean
) : FileJob() {
    @Throws(IOException::class)
    override fun run() {
        val scanInfo = scan(
            path, recursive,
            R.plurals.file_job_set_owner_scan_notification_title_format
        )
        val transferInfo = TransferInfo(scanInfo, null)
        val actionAllInfo = ActionAllInfo()
        walkFileTreeForSettingAttributes(path, recursive, object : SimpleFileVisitor<Path>() {
            @Throws(IOException::class)
            override fun preVisitDirectory(
                directory: Path,
                attributes: BasicFileAttributes
            ): FileVisitResult = visitFile(directory, attributes)

            @Throws(IOException::class)
            override fun visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult {
                setOwner(file, transferInfo, actionAllInfo)
                throwIfInterrupted()
                return FileVisitResult.CONTINUE
            }

            @Throws(IOException::class)
            override fun visitFileFailed(file: Path, exception: IOException): FileVisitResult {
                return super.visitFileFailed(file, exception)
            }
        })
    }

    @Throws(IOException::class)
    private fun setOwner(path: Path, transferInfo: TransferInfo, actionAllInfo: ActionAllInfo) {
        var retry: Boolean
        do {
            retry = false
            try {
                path.setOwner(owner)
                transferInfo.incrementTransferredFileCount()
                postSetOwnerNotification(transferInfo, path)
            } catch (e: InterruptedIOException) {
                throw e
            } catch (e: IOException) {
                e.printStackTrace()
                if (actionAllInfo.skipSetOwnerError) {
                    transferInfo.skipFileIgnoringSize()
                    postSetOwnerNotification(transferInfo, path)
                    return
                }
                if (e is UserActionRequiredException) {
                    val result = showUserAction(e)
                    if (result) {
                        retry = true
                        continue
                    }
                }
                val result = showErrorDialog(
                    getString(R.string.file_job_set_owner_error_title),
                    getString(
                        R.string.file_job_set_owner_error_message_format, getFileName(path),
                        e.toString()
                    ),
                    getReadOnlyFileStore(path, e),
                    true,
                    getString(R.string.retry),
                    getString(R.string.skip),
                    getString(android.R.string.cancel)
                )
                when (result.action) {
                    FileJobErrorAction.POSITIVE -> {
                        retry = true
                        continue
                    }
                    FileJobErrorAction.NEGATIVE -> {
                        if (result.isAll) {
                            actionAllInfo.skipSetOwnerError = true
                        }
                        transferInfo.skipFileIgnoringSize()
                        postSetOwnerNotification(transferInfo, path)
                        return
                    }
                    FileJobErrorAction.CANCELED -> {
                        transferInfo.skipFileIgnoringSize()
                        postSetOwnerNotification(transferInfo, path)
                        return
                    }
                    FileJobErrorAction.NEUTRAL -> throw InterruptedIOException()
                }
            }
        } while (retry)
    }

    private fun postSetOwnerNotification(transferInfo: TransferInfo, currentPath: Path) {
        postTransferCountNotification(
            transferInfo, currentPath, R.string.file_job_set_owner_notification_title_one_format,
            R.plurals.file_job_set_owner_notification_title_multiple_format
        )
    }
}

class SetFileSeLinuxContextJob(
    private val path: Path,
    private val seLinuxContext: String,
    private val recursive: Boolean
) : FileJob() {
    @Throws(IOException::class)
    override fun run() {
        val scanInfo = scan(
            path, recursive,
            R.plurals.file_job_set_selinux_context_scan_notification_title_format
        )
        val transferInfo = TransferInfo(scanInfo, null)
        val actionAllInfo = ActionAllInfo()
        walkFileTreeForSettingAttributes(path, recursive, object : SimpleFileVisitor<Path>() {
            @Throws(IOException::class)
            override fun preVisitDirectory(
                directory: Path,
                attributes: BasicFileAttributes
            ): FileVisitResult = visitFile(directory, attributes)

            @Throws(IOException::class)
            override fun visitFile(file: Path, attributes: BasicFileAttributes): FileVisitResult {
                setSeLinuxContext(file, !attributes.isSymbolicLink, transferInfo, actionAllInfo)
                throwIfInterrupted()
                return FileVisitResult.CONTINUE
            }

            @Throws(IOException::class)
            override fun visitFileFailed(file: Path, exception: IOException): FileVisitResult {
                return super.visitFileFailed(file, exception)
            }
        })
    }

    @Throws(IOException::class)
    private fun setSeLinuxContext(
        path: Path,
        followLinks: Boolean,
        transferInfo: TransferInfo,
        actionAllInfo: ActionAllInfo
    ) {
        var retry: Boolean
        do {
            retry = false
            try {
                val options = if (followLinks) arrayOf() else arrayOf(LinkOption.NOFOLLOW_LINKS)
                path.setSeLinuxContext(seLinuxContext, *options)
                transferInfo.incrementTransferredFileCount()
                postSetSeLinuxContextNotification(transferInfo, path)
            } catch (e: InterruptedIOException) {
                throw e
            } catch (e: IOException) {
                e.printStackTrace()
                if (actionAllInfo.skipSetSeLinuxContextError) {
                    transferInfo.skipFileIgnoringSize()
                    postSetSeLinuxContextNotification(transferInfo, path)
                    return
                }
                if (e is UserActionRequiredException) {
                    val result = showUserAction(e)
                    if (result) {
                        retry = true
                        continue
                    }
                }
                val result = showErrorDialog(
                    getString(R.string.file_job_set_selinux_context_error_title),
                    getString(
                        R.string.file_job_set_selinux_context_error_message_format,
                        getFileName(path), e.toString()
                    ),
                    getReadOnlyFileStore(path, e),
                    true,
                    getString(R.string.retry),
                    getString(R.string.skip),
                    getString(android.R.string.cancel)
                )
                when (result.action) {
                    FileJobErrorAction.POSITIVE -> {
                        retry = true
                        continue
                    }
                    FileJobErrorAction.NEGATIVE -> {
                        if (result.isAll) {
                            actionAllInfo.skipSetSeLinuxContextError = true
                        }
                        transferInfo.skipFileIgnoringSize()
                        postSetSeLinuxContextNotification(transferInfo, path)
                        return
                    }
                    FileJobErrorAction.CANCELED -> {
                        transferInfo.skipFileIgnoringSize()
                        postSetSeLinuxContextNotification(transferInfo, path)
                        return
                    }
                    FileJobErrorAction.NEUTRAL -> throw InterruptedIOException()
                }
            }
        } while (retry)
    }

    private fun postSetSeLinuxContextNotification(transferInfo: TransferInfo, currentPath: Path) {
        postTransferCountNotification(
            transferInfo, currentPath,
            R.string.file_job_set_selinux_context_notification_title_one_format,
            R.plurals.file_job_set_selinux_context_notification_title_multiple_format
        )
    }
}

class WriteFileJob(
    private val file: Path,
    private val content: ByteArray,
    private val listener: ((Boolean) -> Unit)?
) : FileJob() {
    @Throws(IOException::class)
    override fun run() {
        var successful = false
        try {
            file.parent?.createDirectories()
            val channel = file.newByteChannel(
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            )
            channel.use { ByteArrayInputStream(content).copyTo(channel) }
            successful = true
        } finally {
            if (listener != null) {
                mainExecutor.execute { listener(successful) }
            }
        }
    }
}
