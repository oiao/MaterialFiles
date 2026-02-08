/*
 * Copyright (c) 2026 Hai Zhang <dreaming.in.code.zh@gmail.com>
 * All Rights Reserved.
 */

package me.zhanghai.android.files.util

import android.os.Build
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.GeneralSecurityException
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

private const val ANDROID_KEY_STORE = "AndroidKeyStore"
private const val KEY_ALIAS = "me.zhanghai.android.files.preference_encryption_key"
private const val TRANSFORMATION = "AES/GCM/NoPadding"
private const val GCM_TAG_LENGTH_BITS = 128
private const val ENCRYPTED_STRING_PREFIX = "__mf_enc_v1__:"
private val ENCRYPTED_BYTES_MAGIC = byteArrayOf(
    0x4D.toByte(), 0x46.toByte(), 0x45.toByte(), 0x31.toByte()
) // M F E 1

fun ByteArray.encryptForPreferenceStorage(): ByteArray {
    if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) {
        return this
    }
    return try {
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(Cipher.ENCRYPT_MODE, getOrCreateSecretKey())
        val iv = cipher.iv
        val cipherText = cipher.doFinal(this)
        ByteBuffer.allocate(
            ENCRYPTED_BYTES_MAGIC.size + 1 + iv.size + cipherText.size
        ).apply {
            put(ENCRYPTED_BYTES_MAGIC)
            put(iv.size.toByte())
            put(iv)
            put(cipherText)
        }.array()
    } catch (e: GeneralSecurityException) {
        e.printStackTrace()
        this
    }
}

fun ByteArray.decryptForPreferenceStorageIfNeeded(): ByteArray {
    if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M || !startsWithEncryptedMagic()) {
        return this
    }
    return try {
        val buffer = ByteBuffer.wrap(this)
        val magic = ByteArray(ENCRYPTED_BYTES_MAGIC.size)
        buffer.get(magic)
        val ivSize = buffer.get().toInt() and 0xFF
        check(ivSize > 0 && ivSize <= 32) { "Invalid IV length: $ivSize" }
        check(buffer.remaining() > ivSize) { "Invalid encrypted payload" }
        val iv = ByteArray(ivSize)
        buffer.get(iv)
        val cipherText = ByteArray(buffer.remaining())
        buffer.get(cipherText)
        val cipher = Cipher.getInstance(TRANSFORMATION)
        cipher.init(
            Cipher.DECRYPT_MODE,
            getOrCreateSecretKey(),
            GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv)
        )
        cipher.doFinal(cipherText)
    } catch (e: Exception) {
        e.printStackTrace()
        this
    }
}

fun String.encryptForPreferenceStorage(): String =
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
        val encryptedBytes = toByteArray(StandardCharsets.UTF_8).encryptForPreferenceStorage()
        if (encryptedBytes.contentEquals(toByteArray(StandardCharsets.UTF_8))) {
            this
        } else {
            ENCRYPTED_STRING_PREFIX + encryptedBytes.toBase64().value
        }
    } else {
        this
    }

fun String.decryptForPreferenceStorageIfNeeded(): String {
    if (!startsWith(ENCRYPTED_STRING_PREFIX)) {
        return this
    }
    val encryptedBase64 = removePrefix(ENCRYPTED_STRING_PREFIX)
    return try {
        encryptedBase64.asBase64().toByteArray()
            .decryptForPreferenceStorageIfNeeded()
            .toString(StandardCharsets.UTF_8)
    } catch (e: Exception) {
        e.printStackTrace()
        this
    }
}

private fun ByteArray.startsWithEncryptedMagic(): Boolean =
    size > ENCRYPTED_BYTES_MAGIC.size + 1
        && ENCRYPTED_BYTES_MAGIC.indices.all { this[it] == ENCRYPTED_BYTES_MAGIC[it] }

@Throws(GeneralSecurityException::class)
private fun getOrCreateSecretKey(): SecretKey {
    val keyStore = KeyStore.getInstance(ANDROID_KEY_STORE).apply { load(null) }
    val existingKey = keyStore.getKey(KEY_ALIAS, null) as? SecretKey
    if (existingKey != null) {
        return existingKey
    }
    val keyGenerator = KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, ANDROID_KEY_STORE)
    val keySpec = KeyGenParameterSpec.Builder(
        KEY_ALIAS,
        KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT
    )
        .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
        .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
        .setRandomizedEncryptionRequired(true)
        .build()
    keyGenerator.init(keySpec)
    return keyGenerator.generateKey()
}
