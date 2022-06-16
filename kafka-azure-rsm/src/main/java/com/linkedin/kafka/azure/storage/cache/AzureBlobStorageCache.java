/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage.cache;

import java.io.Closeable;
import java.io.InputStream;


/**
 * Interface that provides a way to access input streams to cached Azure Blobs. A concrete
 * implementation will determine how the cache is implemented, and the way in which data is fetched
 * from Azure when a cache miss happens.
 */
public interface AzureBlobStorageCache extends Closeable {
  /**
   * Get an input stream to read an Azure Blob Storage blob which is possibly cached.
   *
   * @param containerName The Azure Blob container from which the blob needs to be fetched.
   * @param blobName The name of the Azure blob that needs to be fetched.
   *
   * @return An input stream to the file stored in the Azure Blob. The class will attempt to serve this
   * stream from the cache, but if the cache does not contain the file,
   * it will fetch it from Azure Blob Storage by making a network call.
   */
  InputStream getInputStream(String containerName, String blobName);

  /**
   * Get an input stream to read an Azure Blob Storage blob which is possibly cached with a defined
   * start position and an optional end position.
   *
   * @param containerName The Azure Blob container from which the blob needs to be fetched.
   * @param blobName The name of the Azure blob that needs to be fetched.
   * @param startPosition The start position from which the blob needs to be read.
   * @param endPosition The end position until which the blob needs to be read. This must be greater than the
   *                    startPosition parameter. If this is greater than the length of the blob, the stream
   *                    will return all bytes until the end of the blob. Clients who simply want to read
   *                    till the end of the blob can thus set this parameter to {@link Integer#MAX_VALUE}.
   * @return An input stream to the file stored in the Azure Blob. The class will attempt to serve this
   * stream from the cache, but if the cache does not contain the file,
   * it will fetch it from Azure Blob Storage by making a network call.
   */
  InputStream getInputStream(String containerName, String blobName, int startPosition, int endPosition);

  /**
   * Mark the cache entry corresponding to an Azure blob as invalid, so that any subsequent calls to
   * {@link #getInputStream(String, String)} or {@link #getInputStream(String, String, int, int)} do not use
   * the cached value, and are forced to fetch the blob from Azure Blob Storage.
   *
   * @param containerName The Azure Blob container of the blob entry that needs to be invalidated.
   * @param blobName The name of the blob that needs to be invalidated.
   */
  void invalidate(String containerName, String blobName);
}
