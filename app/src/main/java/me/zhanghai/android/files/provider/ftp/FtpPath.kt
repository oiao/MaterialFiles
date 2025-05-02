/*
 * Copyright (c) 2022 Hai Zhang <dreaming.in.code.zh@gmail.com>
 * All Rights Reserved.
 */

package me.zhanghai.android.files.provider.ftp

import android.net.Uri
import android.os.Parcel
import android.os.Parcelable
import java8.nio.file.FileSystem
import java8.nio.file.LinkOption
import java8.nio.file.Path
import java8.nio.file.ProviderMismatchException
import java8.nio.file.WatchEvent
import java8.nio.file.WatchKey
import java8.nio.file.WatchService
import me.zhanghai.android.files.provider.common.ByteString
import me.zhanghai.android.files.provider.common.ByteStringListPath
import me.zhanghai.android.files.provider.common.LocalWatchService
import me.zhanghai.android.files.provider.common.UriAuthority
import me.zhanghai.android.files.provider.common.toByteString
import me.zhanghai.android.files.provider.ftp.client.Authority
import me.zhanghai.android.files.provider.ftp.client.Client
import me.zhanghai.android.files.util.readParcelable
import java.io.File
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.channels.SeekableByteChannel
import java.nio.file.OpenOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.FileAttribute

internal class FtpPath : ByteStringListPath<FtpPath>, Client.Path {
    private val fileSystem: FtpFileSystem

    constructor(
        fileSystem: FtpFileSystem,
        path: ByteString
    ) : super(FtpFileSystem.SEPARATOR, path) {
        this.fileSystem = fileSystem
    }

    private constructor(
        fileSystem: FtpFileSystem,
        absolute: Boolean,
        segments: List<ByteString>
    ) : super(FtpFileSystem.SEPARATOR, absolute, segments) {
        this.fileSystem = fileSystem
    }

    override fun isPathAbsolute(path: ByteString): Boolean =
        path.isNotEmpty() && path[0] == FtpFileSystem.SEPARATOR

    override fun createPath(path: ByteString): FtpPath = FtpPath(fileSystem, path)

    override fun createPath(absolute: Boolean, segments: List<ByteString>): FtpPath =
        FtpPath(fileSystem, absolute, segments)

    override val uriScheme: String
        get() = fileSystem.authority.protocol.scheme

    override val uriAuthority: UriAuthority
        get() = fileSystem.authority.toUriAuthority()

    override val uriQuery: ByteString?
        get() =
            Uri.Builder().apply {
                val authority = fileSystem.authority
                if (authority.mode != Authority.DEFAULT_MODE) {
                    appendQueryParameter(QUERY_PARAMETER_MODE, authority.mode.name.lowercase())
                }
                if (authority.encoding != Authority.DEFAULT_ENCODING) {
                    appendQueryParameter(QUERY_PARAMETER_ENCODING, authority.encoding)
                }
            }.build().query?.toByteString()

    override val defaultDirectory: FtpPath
        get() = fileSystem.defaultDirectory

    override fun getFileSystem(): FileSystem = fileSystem

    override fun getRoot(): FtpPath? = if (isAbsolute) fileSystem.rootDirectory else null

    @Throws(IOException::class)
    override fun toRealPath(vararg options: LinkOption): FtpPath {
        throw UnsupportedOperationException()
    }

    override fun toFile(): File {
        throw UnsupportedOperationException()
    }

    @Throws(IOException::class)
    override fun register(
        watcher: WatchService,
        events: Array<WatchEvent.Kind<*>>,
        vararg modifiers: WatchEvent.Modifier
    ): WatchKey {
        if (watcher !is LocalWatchService) {
            throw ProviderMismatchException(watcher.toString())
        }
        return watcher.register(this, events, *modifiers)
    }

    override val authority: Authority
        get() = fileSystem.authority

    override val remotePath: String
        get() = toString()

    override fun newByteChannel(
        options: MutableSet<out OpenOption>,
        vararg attributes: FileAttribute<*>
    ): SeekableByteChannel {
        val isFtpTransfer = true // Always use optimized FTP transfer
        
        val read = options.containsAny(StandardOpenOption.READ)
        val write = options.containsAny(
            StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE,
            StandardOpenOption.CREATE_NEW, StandardOpenOption.TRUNCATE_EXISTING
        )
        val append = options.contains(StandardOpenOption.APPEND)
        
        return Client.openByteChannel(this, append)
    }

    private constructor(source: Parcel) : super(source) {
        fileSystem = source.readParcelable()!!
    }

    override fun writeToParcel(dest: Parcel, flags: Int) {
        super.writeToParcel(dest, flags)

        dest.writeParcelable(fileSystem, flags)
    }

    companion object {
        @JvmField
        val CREATOR = object : Parcelable.Creator<FtpPath> {
            override fun createFromParcel(source: Parcel): FtpPath = FtpPath(source)

            override fun newArray(size: Int): Array<FtpPath?> = arrayOfNulls(size)
        }

        const val QUERY_PARAMETER_MODE = "mode"
        const val QUERY_PARAMETER_ENCODING = "encoding"
    }
}

val Path.isFtpPath: Boolean
    get() = this is FtpPath
