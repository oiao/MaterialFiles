/*
 * Copyright (c) 2022 Hai Zhang <dreaming.in.code.zh@gmail.com>
 * All Rights Reserved.
 */

package me.zhanghai.android.files.provider.ftp.client

import org.apache.commons.net.ftp.FTP
import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.net.ftp.FTPSClient
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

class FtpClient(val authority: Authority) {
    // Increase from default 1KB to 64KB for much better performance
    private val DEFAULT_BUFFER_SIZE = 64 * 1024
    private val KEEP_ALIVE_TIMEOUT = 30 * 1000 // 30 seconds

    var isInUse = false
    var lastActivityTime = System.currentTimeMillis()
    val client: FTPClient = when (authority.protocol) {
        Protocol.FTP -> FTPClient()
        Protocol.FTPS -> FTPSClient()
    }

    fun connect() {
        // Configuration is handled by the Client object
        client.connect(authority.host, authority.port)
    }

    fun inputStream(path: String): InputStream {
        ensureConnected()
        val inputStream = client.retrieveFileStream(path) ?: 
            throw IOException("Failed to retrieve file stream for $path")
        return BufferedInputStream(inputStream, DEFAULT_BUFFER_SIZE)
    }

    fun outputStream(path: String): OutputStream {
        ensureConnected()
        val outputStream = client.storeFileStream(path) ?: 
            throw IOException("Failed to store file stream for $path")
        return BufferedOutputStream(outputStream, DEFAULT_BUFFER_SIZE)
    }

    fun close() {
        if (isInUse) {
            FtpConnectionPool.releaseConnection(this)
        }
    }

    fun disconnect() {
        try {
            client.logout()
        } catch (e: IOException) {
            e.printStackTrace()
        }
        try {
            client.disconnect()
        } catch (e: IOException) {
            e.printStackTrace()
        }
    }

    private fun ensureConnected() {
        if (client.isConnected && System.currentTimeMillis() - lastActivityTime > KEEP_ALIVE_TIMEOUT) {
            try {
                client.sendNoOp()
                lastActivityTime = System.currentTimeMillis()
            } catch (e: IOException) {
                try {
                    disconnect()
                    connect()
                } catch (e: IOException) {
                    throw e
                }
            }
        }
    }
}
