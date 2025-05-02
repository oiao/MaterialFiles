/*
 * Copyright (c) 2019 Hai Zhang <dreaming.in.code.zh@gmail.com>
 * All Rights Reserved.
 */

package me.zhanghai.android.files.ftpserver

import java8.nio.file.Path
import org.apache.ftpserver.ConnectionConfigFactory
import org.apache.ftpserver.FtpServer
import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.ftplet.FtpException
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser
import org.apache.ftpserver.usermanager.impl.WritePermission
import org.apache.ftpserver.DataConnectionConfigurationFactory

class FtpServer(
    private val username: String,
    private val password: String?,
    private val port: Int,
    private val homeDirectory: Path,
    private val writable: Boolean
) {
    private lateinit var server: FtpServer

    @Throws(FtpException::class, RuntimeException::class)
    fun start() {
        server = FtpServerFactory()
            .apply {
                val listener = ListenerFactory()
                    .apply { 
                        port = this@FtpServer.port
                        // Performance optimizations
                        dataConnectionConfiguration = DataConnectionConfigurationFactory().apply {
                            setBufferSize(1024 * 1024) // 1MB buffer for data connections
                            setActiveLocalPort(port + 1) // Active mode port
                            setActiveLocalPortRange(10) // Allow a range of ports for active mode
                        }.createDataConnectionConfiguration()
                    }
                    .createListener()
                addListener("default", listener)
                val user = BaseUser().apply {
                    name = username
                    password = this@FtpServer.password
                    authorities = if (writable) listOf(WritePermission()) else emptyList()
                    homeDirectory = this@FtpServer.homeDirectory.toUri().toString()
                }
                userManager.save(user)
                fileSystem = ProviderFileSystemFactory()
                connectionConfig = ConnectionConfigFactory()
                    .apply { 
                        isAnonymousLoginEnabled = true
                        maxLoginFailures = 5
                        loginFailureDelay = 2000
                    }
                    .createConnectionConfig()
            }
            .createServer()
        server.start()
    }

    fun stop() {
        server.stop()
    }
}
