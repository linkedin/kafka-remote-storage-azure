/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage.cache;

import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.linkedin.kafka.azure.storage.TieredStorageTestUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;


public class AzureBlobStorageMemoryCacheTest {
  private static final long CACHE_SIZE = 8;
  private static final String TEST_CONTAINER = "test-container";
  private static final String TEST_BLOB_1 = "test-blob-1";
  private static final String TEST_BLOB_2 = "test-blob-2";
  private static final String TEST_BLOB_3 = "test-blob-3";
  private AzureBlobStorageMemoryCache cache;
  private AzureBlobStorageMemoryCache.BlobFetcher blobFetcher;

  @BeforeEach
  void setup() {
    blobFetcher = mock(AzureBlobStorageMemoryCache.BlobFetcher.class);
    cache = new AzureBlobStorageMemoryCache(CACHE_SIZE, blobFetcher);
  }

  @Test
  void cacheMissThenSubsequentReadHits() throws IOException {
    final byte[] testBytes = TieredStorageTestUtils.randomBytes(2);
    when(blobFetcher.fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_1))
        .thenReturn(testBytes);
    InputStream inputStream = cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1);
    Assertions.assertArrayEquals(testBytes, inputStream.readAllBytes());

    verify(blobFetcher, times(1)).fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_1);
    verifyNoMoreInteractions(blobFetcher);

    inputStream = cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1);
    Assertions.assertArrayEquals(testBytes, inputStream.readAllBytes());

    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1, 0, 0);

    verifyNoMoreInteractions(blobFetcher);
  }

  @Test
  void cacheLoadThrowsRuntimeException() {
    final BlobStorageException internalErrorException = mock(BlobStorageException.class);
    when(internalErrorException.getErrorCode()).thenReturn(BlobErrorCode.INTERNAL_ERROR);

    when(blobFetcher.fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_1))
        .thenThrow(internalErrorException);
    Assertions.assertThrows(BlobStorageException.class, () -> cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1));
  }

  @Test
  void inputStreamStartAndEndOffsets() throws IOException {
    final byte[] testBytes = TieredStorageTestUtils.randomBytes(4);
    when(blobFetcher.fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_1))
        .thenReturn(testBytes);

    InputStream inputStream = cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1, 0, 3);
    Assertions.assertEquals(ByteBuffer.wrap(testBytes, 0, 4), ByteBuffer.wrap(inputStream.readAllBytes()));

    inputStream = cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1, 2, 3);
    Assertions.assertEquals(ByteBuffer.wrap(testBytes, 2, 2), ByteBuffer.wrap(inputStream.readAllBytes()));

    inputStream = cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1, 1, 2);
    Assertions.assertEquals(ByteBuffer.wrap(testBytes, 1, 2), ByteBuffer.wrap(inputStream.readAllBytes()));

    inputStream = cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1, 1, 10);
    Assertions.assertEquals(ByteBuffer.wrap(testBytes, 1, 3), ByteBuffer.wrap(inputStream.readAllBytes()));
  }

  @Test
  void throwsWhenStartOffsetIsIllegal() {
    final byte[] testBytes = TieredStorageTestUtils.randomBytes(4);
    when(blobFetcher.fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_1))
        .thenReturn(testBytes);

    Assertions.assertThrows(IllegalArgumentException.class,
        () -> cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1, 4, Integer.MAX_VALUE));
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1, 2, 1));
  }

  @Test
  void cacheSizeIsBounded() {
    when(blobFetcher.fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_1))
        .thenReturn(TieredStorageTestUtils.randomBytes(4));
    when(blobFetcher.fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_2))
        .thenReturn(TieredStorageTestUtils.randomBytes(4));
    when(blobFetcher.fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_3))
        .thenReturn(TieredStorageTestUtils.randomBytes(4));

    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1);
    Assertions.assertEquals(1, cache.numberOfEntries());

    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_2);
    Assertions.assertEquals(2, cache.numberOfEntries());

    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1);
    Assertions.assertEquals(2, cache.numberOfEntries());

    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_3);
    Assertions.assertEquals(2, cache.numberOfEntries());

    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_2);
    Assertions.assertEquals(2, cache.numberOfEntries());
  }

  @Test
  void removeSucceeds() {
    when(blobFetcher.fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_1))
        .thenReturn(TieredStorageTestUtils.randomBytes(4));
    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1);
    verify(blobFetcher, times(1)).fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_1);
    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1);
    verifyNoMoreInteractions(blobFetcher);
    reset(blobFetcher);

    when(blobFetcher.fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_1))
        .thenReturn(TieredStorageTestUtils.randomBytes(4));
    cache.invalidate(TEST_CONTAINER, TEST_BLOB_1);
    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1);
    verify(blobFetcher, times(1)).fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_1);
  }

  @Test
  void cacheSizeIsBoundedWithDifferentWeights() {
    when(blobFetcher.fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_1))
        .thenReturn(TieredStorageTestUtils.randomBytes(4));
    when(blobFetcher.fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_2))
        .thenReturn(TieredStorageTestUtils.randomBytes(4));
    when(blobFetcher.fetchBlobAsBytes(TEST_CONTAINER, TEST_BLOB_3))
        .thenReturn(TieredStorageTestUtils.randomBytes(6));

    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1);
    Assertions.assertEquals(1, cache.numberOfEntries());

    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_2);
    Assertions.assertEquals(2, cache.numberOfEntries());

    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1);
    Assertions.assertEquals(2, cache.numberOfEntries());

    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_3);
    Assertions.assertEquals(1, cache.numberOfEntries());

    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_1);
    Assertions.assertEquals(1, cache.numberOfEntries());

    cache.getInputStream(TEST_CONTAINER, TEST_BLOB_2);
    Assertions.assertEquals(2, cache.numberOfEntries());
  }
}
