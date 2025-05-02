// ...existing code...

// Add these constants at the top of the class
companion object {
    // Increase from default 1KB to 64KB for much better performance
    private const val DEFAULT_BUFFER_SIZE = 64 * 1024
    private const val KEEP_ALIVE_TIMEOUT = 30 * 1000 // 30 seconds
}

// Add these fields to the class
var isInUse = false
var lastActivityTime = System.currentTimeMillis()

// Modify the inputStream method to use buffered streams with larger buffer
fun inputStream(path: String): InputStream {
    ensureConnected()
    val inputStream = if (client is FTPSClient) {
        client.retrieveFileStream(path)
    } else {
        client.retrieveFileStream(path)
    }
    return BufferedInputStream(inputStream, DEFAULT_BUFFER_SIZE)
}

// Modify the outputStream method to use buffered streams
fun outputStream(path: String): OutputStream {
    ensureConnected()
    val outputStream = if (client is FTPSClient) {
        client.storeFileStream(path)
    } else {
        client.storeFileStream(path)
    }
    return BufferedOutputStream(outputStream, DEFAULT_BUFFER_SIZE)
}

// Modify the close method to release connection back to pool
override fun close() {
    if (isInUse) {
        FtpConnectionPool.releaseConnection(this)
    }
}

// Set transfer mode to binary
fun connect() {
    // ...existing connect code...
    
    // Always use binary mode for optimal transfer speed
    client.setFileType(FTP.BINARY_FILE_TYPE)
    
    // Set buffer sizes
    client.bufferSize = DEFAULT_BUFFER_SIZE
    
    // Enable keep-alive if supported
    try {
        client.setControlKeepAliveTimeout(15) // 15 second timeout
    } catch (e: Exception) {
        // Ignore if not supported
    }
    
    lastActivityTime = System.currentTimeMillis()
}

// Add method to ensure client stays connected
private fun ensureConnectionActive() {
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
// ...existing code...
