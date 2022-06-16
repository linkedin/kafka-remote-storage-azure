/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

/** In-memory implementation of cache for Azure Blobs. This class needs to be initialized with
 * a blob fetcher that is used to fetch a blob if it is missing from the cache.
 *
 * For the cache implementation, it uses the Caffeine library, which provides an approximate least-frequently used
 * cache eviction strategy.
 *
 * @see <a href="https://arxiv.org/pdf/1512.00727.pdf">TinyLFU: A Highly Efficient Cache Admission Policy</a>
 */
/* TODO: Emit metrics for these -
    number of entries
    number of bytes
    insertion entries and bytes rate
    eviction entries and bytes rate
    hitRatio
 */
public class AzureBlobStorageMemoryCache implements AzureBlobStorageCache {
  /**
   * This functional interface can be implemented by a class to determine how the blob is fetched.
   */
  public interface BlobFetcher {
    /**
     * Fetch all the bytes contained in a blob in Azure Blob Storage.
     * @param containerName Name of the Azure Blob Storage container containing the blob.
     * @param blobName Name of the blob to be fetched.
     * @return Array of fetched blob bytes.
     */
    byte[] fetchBlobAsBytes(String containerName, String blobName);
  }

  private final LoadingCache<AzureLocation, byte[]> cache;
  public AzureBlobStorageMemoryCache(final long maxBytes, final BlobFetcher blobBytesFetcher) {
    cache = Caffeine.newBuilder()
        .maximumWeight(maxBytes)
        .weigher((AzureLocation key, byte[] value) -> value.length)
        .build(key -> {
          final String containerName = key.getContainerName();
          final String blobName = key.getBlobName();
          return blobBytesFetcher.fetchBlobAsBytes(containerName, blobName);
        });
  }

  /**
   * Get the number of entries that the cache currently contains. This method is to be only used for testing.
   *
   * @return Number of blobs entries stored in the cache.
   */
  long numberOfEntries() {
    cache.cleanUp();
    return cache.estimatedSize();
  }

  @Override
  public InputStream getInputStream(String containerName, String blobName) {
    final byte[] data = cache.get(new AzureLocation(containerName, blobName));
    return new ByteArrayInputStream(data);
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException if startPosition is greater than the length of the blob.
   */
  @Override
  public InputStream getInputStream(String containerName, String blobName, int startPosition, int endPosition) {
    final byte[] data = cache.get(new AzureLocation(containerName, blobName));

    if (startPosition >= data.length) {
      throw new IllegalArgumentException(
          String.format("start position %d must be less than segment length %d",
              startPosition, data.length));
    }

    if (endPosition < startPosition) {
      throw new IllegalArgumentException("End position must be greater than or equal to the start position");
    }

    // The number of bytes to read from the segment must not be more than the size of the segment (duh!)
    // The interface post-condition requires that the returned InputStream will end at the smaller of
    // endPosition and the end of the remote log segment data file/object. Note, endPosition is inclusive.
    // Therefore, minimum of data.length-1 (which is the highest possible byte offset that can be
    // returned) and the input endPosition is taken below.
    int length = Math.min(data.length - 1, endPosition) - startPosition + 1;

    return new ByteArrayInputStream(data, startPosition, length);
  }

  @Override
  public void invalidate(String containerName, String blobName) {
    cache.invalidate(new AzureLocation(containerName, blobName));
  }

  @Override
  public void close() {
    // No-op. Nothing to close.
  }
}
