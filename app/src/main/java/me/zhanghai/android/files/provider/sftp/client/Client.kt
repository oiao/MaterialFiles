/*
 * Copyright (c) 2021 Hai Zhang <dreaming.in.code.zh@gmail.com>
 * All Rights Reserved.
 */

package me.zhanghai.android.files.provider.sftp.client

import java8.nio.channels.SeekableByteChannel
import me.zhanghai.android.files.provider.common.LocalWatchService
import me.zhanghai.android.files.provider.common.NotifyEntryModifiedSeekableByteChannel
import me.zhanghai.android.files.util.closeSafe
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.sftp.FileAttributes
import net.schmizz.sshj.sftp.FileMode
import net.schmizz.sshj.sftp.OpenMode
import net.schmizz.sshj.sftp.RemoteFile
import net.schmizz.sshj.sftp.Response
import net.schmizz.sshj.sftp.SFTPClient
import net.schmizz.sshj.sftp.SFTPException
import net.schmizz.sshj.transport.TransportException
import net.schmizz.sshj.userauth.UserAuthException
import java.io.IOException
import java.util.Collections
import java.util.WeakHashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java8.nio.file.Path as Java8Path
import java.nio.channels.AsynchronousCloseException
import java.nio.channels.ClosedByInterruptException

object Client {
    // Optimized buffer sizes for faster transfer
    private const val SFTP_BUFFER_SIZE = 262144 // 256KB
    private const val MAX_IDLE_CONNECTIONS = 5
    private const val CONNECTION_TIMEOUT_MILLIS = 30000L // 30 seconds

    @Volatile
    lateinit var authenticator: Authenticator

    private class ClientConnection(
        val client: SSHClient,
        val sftpClient: SFTPClient,
        var lastUsedTime: Long = System.currentTimeMillis()
    )

    // Connection pooling for better performance with multiple transfers
    private val clientPool = ConcurrentHashMap<Authority, MutableList<ClientConnection>>()
    private val directoryFileAttributesCache = Collections.synchronizedMap(
        WeakHashMap<Path, FileAttributes>()
    )

    @Throws(IOException::class)
    private fun acquireClient(authority: Authority): Pair<SSHClient, SFTPClient> {
        // Try to reuse an existing connection
        val clientConnection = synchronized(clientPool) {
            val connections = clientPool[authority]
            if (connections != null && connections.isNotEmpty()) {
                val connection = connections.removeAt(connections.size - 1)
                if (connections.isEmpty()) {
                    clientPool.remove(authority)
                }
                connection
            } else null
        }

        if (clientConnection != null) {
            try {
                // Make sure the connection is still valid
                if (clientConnection.client.isConnected && clientConnection.client.isAuthenticated) {
                    clientConnection.lastUsedTime = System.currentTimeMillis()
                    return clientConnection.client to clientConnection.sftpClient
                }
            } catch (e: Exception) {
                e.printStackTrace()
                try {
                    clientConnection.sftpClient.close()
                    clientConnection.client.close()
                } catch (e2: Exception) {
                    e2.printStackTrace()
                }
            }
        }

        // Create a new connection
        return createClient(authority)
    }

    @Throws(IOException::class)
    private fun createClient(authority: Authority): Pair<SSHClient, SFTPClient> {
        val authentication = authenticator.getAuthentication(authority)
            ?: throw UserAuthException("No authentication found for $authority")
        val client = SSHClient()
        
        // Configure client for optimal performance
        client.connectTimeout = CONNECTION_TIMEOUT_MILLIS
        client.socket.soTimeout = CONNECTION_TIMEOUT_MILLIS.toInt()
        
        // Enable compression for better performance
        client.useCompression()
        
        // Configure socket parameters for better performance
        client.socket.receiveBufferSize = SFTP_BUFFER_SIZE
        client.socket.sendBufferSize = SFTP_BUFFER_SIZE
        client.socket.tcpNoDelay = true
        
        // Set timeout so we don't hang
        client.setTimeout(CONNECTION_TIMEOUT_MILLIS)
        client.connectTimeout = CONNECTION_TIMEOUT_MILLIS
        
        // Configure preferred faster ciphers
        client.config.preferredCiphers = listOf(
            "aes128-ctr", "aes192-ctr", "aes256-ctr" // Faster ciphers first
        )
        
        try {
            client.connect(authority.host, authority.port)
            authentication.authenticate(client, authority)
            if (!client.isAuthenticated) {
                throw UserAuthException("Authentication failed for $authority")
            }
            client.timeout = (authority.connectTimeout ?: CONNECTION_TIMEOUT_MILLIS).toInt()
            // Optimize SFTP configuration for better performance
            val sftpClient = client.newSFTPClient()
            sftpClient.sftpEngine.packetSize = SFTP_BUFFER_SIZE
            return client to sftpClient
        } catch (e: Exception) {
            client.disconnect()
            throw e
        }
    }

    private fun releaseClient(
        authority: Authority,
        client: SSHClient,
        sftpClient: SFTPClient
    ) {
        synchronized(clientPool) {
            try {
                if (!client.isConnected) {
                    return
                }
                
                val connections = clientPool.getOrPut(authority) { mutableListOf() }
                // Limit the pool size per authority
                if (connections.size >= MAX_IDLE_CONNECTIONS) {
                    // Remove and close oldest connection
                    val oldestConnection = connections.minByOrNull { it.lastUsedTime }
                    if (oldestConnection != null) {
                        connections.remove(oldestConnection)
                        try {
                            oldestConnection.sftpClient.close()
                            oldestConnection.client.close()
                        } catch (e: IOException) {
                            e.printStackTrace()
                        }
                    }
                }
                connections.add(ClientConnection(client, sftpClient, System.currentTimeMillis()))
            } catch (e: Exception) {
                e.printStackTrace()
                try {
                    sftpClient.close()
                    client.disconnect()
                } catch (e2: Exception) {
                    e2.printStackTrace()
                }
            }
        }
    }

    private inline fun <R> useClient(authority: Authority, block: (SFTPClient) -> R): R {
        val (client, sftpClient) = acquireClient(authority)
        try {
            return block(sftpClient)
        } finally {
            releaseClient(authority, client, sftpClient)
        }
    }

    @Throws(ClientException::class)
    fun access(path: Path, flags: Set<OpenMode>) {
        try {
            useClient(path.authority) { it.open(path.remotePath, flags, FileAttributes.EMPTY).close() }
        } catch (e: IOException) {
            throw ClientException(e)
        }
    }

    @Throws(ClientException::class)
    fun lstat(path: Path): FileAttributes {
        synchronized(directoryFileAttributesCache) {
            directoryFileAttributesCache[path]?.let {
                return it.also { directoryFileAttributesCache -= path }
            }
        }
        return try {
            useClient(path.authority) { it.lstat(path.remotePath) }
        } catch (e: IOException) {
            throw ClientException(e)
        }
    }

    @Throws(ClientException::class)
    fun mkdir(path: Path, attributes: FileAttributes) {
        try {
            useClient(path.authority) { it.mkdir(path.remotePath, attributes) }
        } catch (e: IOException) {
            throw ClientException(e)
        }
        LocalWatchService.onEntryCreated(path as Java8Path)
    }

    @Throws(ClientException::class)
    fun openByteChannel(
        path: Path,
        flags: Set<OpenMode>,
        attributes: FileAttributes
    ): SeekableByteChannel {
        val file = try {
            useClient(path.authority) { it.open(path.remotePath, flags, attributes) }
        } catch (e: IOException) {
            throw ClientException(e)
        }
        return NotifyEntryModifiedSeekableByteChannel(
            FileByteChannel(file, flags.contains(OpenMode.APPEND)), path as Java8Path
        )
    }

    @Throws(ClientException::class)
    fun readlink(path: Path): String {
        return try {
            useClient(path.authority) { it.readlink(path.remotePath) }
        } catch (e: IOException) {
            throw ClientException(e)
        }
    }

    @Throws(ClientException::class)
    fun realpath(path: Path): Path {
        val realPath = try {
            useClient(path.authority) { it.canonicalize(path.remotePath) }
        } catch (e: IOException) {
            throw ClientException(e)
        }
        return path.resolve(realPath)
    }

    @Throws(ClientException::class)
    fun remove(path: Path) {
        val attributes = lstat(path)
        val isDirectory = attributes.type == FileMode.Type.DIRECTORY
        if (isDirectory) {
            rmdir(path)
        } else {
            unlink(path)
        }
    }

    @Throws(ClientException::class)
    fun rename(path: Path, newPath: Path) {
        if (newPath.authority != path.authority) {
            throw ClientException(
                SFTPException(Response.StatusCode.FAILURE, "Paths aren't on the same authority")
            )
        }
        try {
            useClient(path.authority) { it.rename(path.remotePath, newPath.remotePath) }
        } catch (e: IOException) {
            throw ClientException(e)
        }
        directoryFileAttributesCache -= path
        directoryFileAttributesCache -= newPath
        LocalWatchService.onEntryDeleted(path as Java8Path)
        LocalWatchService.onEntryCreated(newPath as Java8Path)
    }

    @Throws(ClientException::class)
    fun rmdir(path: Path) {
        try {
            useClient(path.authority) { it.rmdir(path.remotePath) }
        } catch (e: IOException) {
            throw ClientException(e)
        }
        directoryFileAttributesCache -= path
        LocalWatchService.onEntryDeleted(path as Java8Path)
    }

    @Throws(ClientException::class)
    fun scandir(path: Path): List<Path> {
        val files = try {
            useClient(path.authority) { it.ls(path.remotePath) }
        } catch (e: IOException) {
            throw ClientException(e)
        }
        return files.map { file ->
            path.resolve(file.name).also { directoryFileAttributesCache[it] = file.attributes }
        }
    }

    @Throws(ClientException::class)
    fun setstat(path: Path, attributes: FileAttributes) {
        try {
            useClient(path.authority) { it.setattr(path.remotePath, attributes) }
        } catch (e: IOException) {
            throw ClientException(e)
        }
        directoryFileAttributesCache -= path
        LocalWatchService.onEntryModified(path as Java8Path)
    }

    @Throws(ClientException::class)
    fun stat(path: Path): FileAttributes {
        synchronized(directoryFileAttributesCache) {
            directoryFileAttributesCache[path]?.let {
                if (it.type != FileMode.Type.SYMLINK) {
                    return it.also { directoryFileAttributesCache -= path }
                }
            }
        }
        return try {
            useClient(path.authority) { it.stat(path.remotePath) }
        } catch (e: IOException) {
            throw ClientException(e)
        }
    }

    @Throws(ClientException::class)
    fun symlink(link: Path, target: String) {
        try {
            useClient(link.authority) { it.symlink(link.remotePath, target) }
        } catch (e: IOException) {
            throw ClientException(e)
        }
        LocalWatchService.onEntryCreated(link as Java8Path)
    }

    @Throws(ClientException::class)
    fun unlink(path: Path) {
        try {
            useClient(path.authority) { it.rm(path.remotePath) }
        } catch (e: IOException) {
            throw ClientException(e)
        }
        directoryFileAttributesCache -= path
        LocalWatchService.onEntryDeleted(path as Java8Path)
    }

    interface Path {
        val authority: Authority
        val remotePath: String
        fun resolve(other: String): Path
    }
}
