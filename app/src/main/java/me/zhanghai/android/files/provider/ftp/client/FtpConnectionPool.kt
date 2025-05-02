/*
 * Copyright (c) 2022 Hai Zhang <dreaming.in.code.zh@gmail.com>
 * All Rights Reserved.
 */

package me.zhanghai.android.files.provider.ftp.client

import me.zhanghai.android.files.provider.ftp.Authority
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

object FtpConnectionPool {
    private val connections = ConcurrentHashMap<Authority, MutableList<FtpClient>>()
    private const val MAX_CONNECTIONS_PER_HOST = 5
    private const val CONNECTION_TIMEOUT = 60L // seconds
    private val executorService: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    
    init {
        // Schedule cleanup task
        executorService.scheduleAtFixedRate(
            { cleanIdleConnections() },
            CONNECTION_TIMEOUT,
            CONNECTION_TIMEOUT,
            TimeUnit.SECONDS
        )
    }
    
    @Synchronized
    fun getConnection(authority: Authority): FtpClient {
        val clientList = connections.getOrPut(authority) { mutableListOf() }
        
        // Try to find an idle connection
        val client = clientList.firstOrNull { !it.isInUse }
        
        return when {
            client != null -> {
                client.isInUse = true
                client
            }
            clientList.size < MAX_CONNECTIONS_PER_HOST -> {
                // Create new connection if below limit
                val newClient = FtpClient(authority)
                newClient.isInUse = true
                try {
                    newClient.connect()
                    clientList.add(newClient)
                    newClient
                } catch (e: IOException) {
                    newClient.isInUse = false
                    throw e
                }
            }
            else -> {
                // Wait for an available connection
                throw IOException("Too many connections to ${authority}")
            }
        }
    }
    
    @Synchronized
    fun releaseConnection(client: FtpClient) {
        client.isInUse = false
        client.lastActivityTime = System.currentTimeMillis()
    }
    
    private fun cleanIdleConnections() {
        val now = System.currentTimeMillis()
        connections.forEach { (authority, clients) ->
            synchronized(this) {
                clients.removeAll { client ->
                    val isIdle = !client.isInUse && 
                               now - client.lastActivityTime > TimeUnit.SECONDS.toMillis(CONNECTION_TIMEOUT)
                    if (isIdle) {
                        try {
                            client.disconnect()
                        } catch (e: IOException) {
                            e.printStackTrace()
                        }
                    }
                    isIdle
                }
                if (clients.isEmpty()) {
                    connections.remove(authority)
                }
            }
        }
    }
}
