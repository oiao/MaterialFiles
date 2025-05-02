/*
 * Copyright (c) 2021 Hai Zhang <dreaming.in.code.zh@gmail.com>
 * All Rights Reserved.
 */

package net.schmizz.sshj.sftp;

import net.schmizz.concurrent.Promise;

import java.io.IOException;

import androidx.annotation.NonNull;

public class RemoteFileAccessor {
    private RemoteFileAccessor() {}

    // Increased buffer size for better network throughput
    private static final int BUFFER_SIZE = 65536; // 64KB buffer for better performance

    @NonNull
    public static Promise<Response, SFTPException> asyncRead(@NonNull RemoteFile file, long offset,
                                                             int length) throws IOException {
        // Use our optimized buffer size if length is larger
        int optimalLength = Math.min(length, BUFFER_SIZE);
        return file.asyncRead(offset, optimalLength);
    }

    @NonNull
    public static SFTPEngine getRequester(@NonNull RemoteFile file) {
        return file.requester;
    }
}
