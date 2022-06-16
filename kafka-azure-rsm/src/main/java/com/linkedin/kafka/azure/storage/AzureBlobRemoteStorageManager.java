/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import com.azure.storage.blob.BlobServiceClient;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.linkedin.kafka.azure.storage.cache.AzureBlobStorageCache;
import com.linkedin.kafka.azure.storage.cache.AzureBlobStorageMemoryCache;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is an implementation of the KIP-405's (Tiered Storage) RemoteStorageManager interface
 */
public class AzureBlobRemoteStorageManager implements RemoteStorageManager {
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobRemoteStorageManager.class);
  private static final String WRITE_LATENCY_MILLIS = "WRITE_LATENCY_MILLIS";
  private static final String BYTES_OUT_RATE = "BYTES_OUT_RATE";

  private ContainerNameEncoder containerNameEncoder;
  private AzureBlobStorageClient azureBlobStorageClient;
  private AzureBlobStorageCache cache;

  private final MetricRegistry metricRegistry = new MetricRegistry();
  private final Timer writeLatencyTimer = metricRegistry.timer(WRITE_LATENCY_MILLIS);
  private final Meter bytesOutRateMeter = metricRegistry.meter(BYTES_OUT_RATE);

  public AzureBlobRemoteStorageManager() {
  }

  // Visible only for testing
  AzureBlobRemoteStorageManager(
      ContainerNameEncoder containerNameEncoder,
      AzureBlobStorageClient azureBlobStorageClient,
      AzureBlobStorageCache azureBlobStorageCache) {
    this.containerNameEncoder = containerNameEncoder;
    this.azureBlobStorageClient = azureBlobStorageClient;
    this.cache = azureBlobStorageCache;
  }

  @Override
  public void close() throws IOException {
    LOG.info("Shutting down {}", this.getClass().getSimpleName());
    cache.close();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    LOG.info("Setting up {} with configs: ", this.getClass().getSimpleName());
    for (Map.Entry<String, ?> entry: configs.entrySet()) {
      LOG.info("{}: {} ", entry.getKey(), entry.getValue());
    }
    AzureBlobRemoteStorageConfig config = new AzureBlobRemoteStorageConfig(configs);
    containerNameEncoder = new ContainerNameEncoder(config.getContainerNamePrefix());
    BlobServiceClient blobServiceClient =
        BlobServiceClientBuilderFactory.getBlobServiceClientBuilder(config).buildClient();
    // This is non-null only during testing
    azureBlobStorageClient = new AzureBlobStorageClient(
        blobServiceClient, config.getMaxSingleUploadSize(), config.getBlockSize());
    cache = buildCache(config);

    ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry).build();
    reporter.start(600, TimeUnit.SECONDS);
    reporter.report();
  }

  private AzureBlobStorageCache buildCache(AzureBlobRemoteStorageConfig config) {
    return new AzureBlobStorageMemoryCache(config.getCacheBytes(), (containerName, blobName) -> {
      final byte[] bytes = azureBlobStorageClient.fetchBlobAsBytes(containerName, blobName);
      LOG.debug("Fetched {} bytes from Azure for blob {} in container {}", bytes, blobName, containerName);
      return bytes;
    });
  }

  static Optional<Path> getIndexFilePath(LogSegmentData logSegmentData, RemoteStorageManager.IndexType indexType) {
    switch (indexType) {
      case OFFSET:
        return Optional.of(logSegmentData.offsetIndex());
      case PRODUCER_SNAPSHOT:
        return Optional.of(logSegmentData.producerSnapshotIndex());
      case TIMESTAMP:
        return Optional.of(logSegmentData.timeIndex());
      case TRANSACTION:
        return logSegmentData.transactionIndex();
      default:
        throw new IllegalArgumentException(String.format("index type %s does not have a file path", indexType));
    }
  }

  private long uploadIndexToBlob(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
      final String containerName, IndexType indexType, LogSegmentData logSegmentData) throws AzureBlobStorageException {
    final String blobName = getBlobNameForIndex(remoteLogSegmentMetadata, indexType);
    try {
      if (indexType == IndexType.LEADER_EPOCH) {
        return azureBlobStorageClient.uploadBytesToBlob(containerName, blobName, logSegmentData.leaderEpochIndex().array());
      } else {
        final Optional<Path> indexFilePathOptional = getIndexFilePath(logSegmentData, indexType);
        return indexFilePathOptional.map(indexFilePath ->
            azureBlobStorageClient.uploadFileToBlob(containerName, blobName, indexFilePath)).orElse(0L);
      }
    } catch (Exception e) {
      throw new AzureBlobStorageException("Could not upload index {} to blob {} in container {} for log segment {}",
          indexType, blobName, containerName, remoteLogSegmentMetadata, e);
    }
  }

  private long uploadLogSegmentToBlob(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
      final String containerName, Path inputFile, IndexType indexType) throws AzureBlobStorageException {
    final String blobName = getBlobNameForSegment(remoteLogSegmentMetadata);
    try {
      return azureBlobStorageClient.uploadFileToBlob(containerName, blobName, inputFile);
    } catch (Exception e) {
      throw new AzureBlobStorageException("Could not upload log segment to blob {} in container {} for log segment {}",
          blobName, containerName, remoteLogSegmentMetadata, e);
    }
  }

  /* Visible only for testing */
  boolean containsFile(RemoteLogSegmentMetadata remoteLogSegmentMetadata, String fileName) {
    String containerName = getContainerName(remoteLogSegmentMetadata);
    return azureBlobStorageClient.doesBlobExist(containerName, fileName);
  }

  static String getBlobNameForSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
    return getBlobName(remoteLogSegmentMetadata, "SEGMENT");
  }

  static String getBlobNameForIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata, IndexType indexType) {
    return getBlobName(remoteLogSegmentMetadata, indexType.toString());
  }

  static String getBlobName(RemoteLogSegmentMetadata remoteLogSegmentMetadata, String suffix) {
    // Azure blob name requirements
    // https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#blob-names
    int partition = remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition().partition();
    // kafka.common.Uuid.toString() uses Base64 encoding, which may contain '/' and '+'. They are valid in blob names.
    // However, we use canonical UUID naming for simplicity.
    Uuid id = remoteLogSegmentMetadata.remoteLogSegmentId().id();
    String logSegmentId = new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits()).toString();
    return String.format("%d.%s.%s", partition, logSegmentId, suffix);
  }

  String getContainerName(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
    return containerNameEncoder.encode(remoteLogSegmentMetadata);
  }

  @Override
  public void copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
      LogSegmentData logSegmentData) throws RemoteStorageException {
    LOG.debug("Writing remote log segment started for {}", remoteLogSegmentMetadata);

    if (remoteLogSegmentMetadata == null) {
      throw new NullPointerException("remoteLogSegmentMetadata must be non-null");
    }
    if (logSegmentData == null) {
      throw new NullPointerException(
          String.format("logSegmentData must be non-null for remote log segment %s", remoteLogSegmentMetadata));
    }
    String containerName = getContainerName(remoteLogSegmentMetadata);

    Timer.Context timeContext = writeLatencyTimer.time();
    long totalBytesUploaded = 0;
    try {
      totalBytesUploaded += uploadLogSegmentToBlob(remoteLogSegmentMetadata, containerName,
          logSegmentData.logSegment(), null);
      for (IndexType indexType : IndexType.values()) {
        totalBytesUploaded += uploadIndexToBlob(remoteLogSegmentMetadata, containerName,
            indexType, logSegmentData);
      }

      LOG.debug("Writing remote log segment completed for {}", remoteLogSegmentMetadata);
      bytesOutRateMeter.mark(totalBytesUploaded);
    } finally {
      timeContext.stop();
    }
  }

  @Override
  public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int startPosition) throws RemoteStorageException {
    if (remoteLogSegmentMetadata == null) {
      throw new NullPointerException("RemoteLogSegmentMetadata must be non-null");
    }
    return fetchLogSegment(remoteLogSegmentMetadata, startPosition, Integer.MAX_VALUE);
  }

  @Override
  public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                     int startPosition, int endPosition) throws RemoteStorageException {
    if (remoteLogSegmentMetadata == null) {
      throw new NullPointerException("RemoteLogSegmentMetadata must be non-null");
    }
    LOG.debug("Received fetch segment request from [{}-{}] for segment {}", startPosition, endPosition, remoteLogSegmentMetadata);

    if (startPosition < 0 || endPosition < 0) {
      throw new IllegalArgumentException("Start position and end position must >= 0");
    }

    if (endPosition < startPosition) {
      throw new IllegalArgumentException("End position must be greater than or equal to the start position");
    }

    String segmentBlobName = getBlobNameForSegment(remoteLogSegmentMetadata);
    String containerName = getContainerName(remoteLogSegmentMetadata);
    try {
      return cache.getInputStream(containerName, segmentBlobName, startPosition, endPosition);
    } catch (ResourceNotFoundException e) {
      throw new AzureResourceNotFoundException("Remote log segment {} does not exist at blob {} in container {}",
          remoteLogSegmentMetadata, segmentBlobName, containerName, e);
    } catch (Exception e) {
      throw new AzureBlobStorageException("Remote log segment {} could not be fetched from blob {} in container {}",
          remoteLogSegmentMetadata, segmentBlobName, containerName, e);
    }
  }

  @Override
  public InputStream fetchIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                IndexType indexType) throws RemoteStorageException {
    LOG.debug("Fetching index type {} and segment {}", indexType, remoteLogSegmentMetadata);
    if (remoteLogSegmentMetadata == null) {
      throw new NullPointerException("RemoteLogSegmentMetadata can be non-null");
    }
    if (indexType == null) {
      throw new NullPointerException(String.format("indexType must be non-null for remote log segment %s", remoteLogSegmentMetadata));
    }

    String indexBlobName = getBlobNameForIndex(remoteLogSegmentMetadata, indexType);
    String containerName = getContainerName(remoteLogSegmentMetadata);
    try {
       return cache.getInputStream(containerName, indexBlobName);
    } catch (ResourceNotFoundException e) {
      throw new AzureResourceNotFoundException(
          "Remote index {} corresponding to log segment {} does not exist at blob {} in container {}",
          indexType, remoteLogSegmentMetadata, indexBlobName, containerName);
    } catch (Exception e) {
      throw new AzureBlobStorageException("Remote index {} corresponding to log segment {} could not be fetched",
          indexType, remoteLogSegmentMetadata, e);
    }
  }

  /**
   * Delete a blob from the Azure blob store.
   * @param remoteLogSegmentMetadata metadata corresponding to the log segment to which the blob being deleted belongs
   * @param containerName Name of the Azure Blob Storage container in which the blob to be deleted resides.
   * @param blobName Name of the blob to delete.
   * @return true if deletion is successful, false otherwise.
   */
  private boolean deleteBlob(RemoteLogSegmentMetadata remoteLogSegmentMetadata, String containerName, String blobName) {
    try {
      azureBlobStorageClient.deleteBlob(containerName, blobName);
      cache.invalidate(containerName, blobName);
      return true;
    } catch (Exception ex) {
      LOG.error("Error deleting blob {} in container {} for remote log segment {}",
          blobName, containerName, remoteLogSegmentMetadata, ex);
      return false;
    }
  }

  @Override
  public void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
    Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

    final String containerName = getContainerName(remoteLogSegmentMetadata);
    LOG.debug("Started deletion of blobs in container {} for the remote log segment {}",
        containerName, remoteLogSegmentMetadata);
    boolean allDeletesSucceeded =
        deleteBlob(remoteLogSegmentMetadata, containerName, getBlobNameForSegment(remoteLogSegmentMetadata));

    for (IndexType indexType: IndexType.values()) {
      String indexBlob = getBlobNameForIndex(remoteLogSegmentMetadata, indexType);

      // In linkedin we don't use Kafka transactions but there should be a better way
      // to detect if the transaction index was ever created or not.
      // Perhaps RemoteLogSegmentMetadata should include a byte[] for arbitrary data.
      // For now, we will just call deleteBlob, which ignores a BLOB_NOT_FOUND error.
      if (!deleteBlob(remoteLogSegmentMetadata, containerName, indexBlob)) {
        allDeletesSucceeded = false;
      }
    }

    if (allDeletesSucceeded) {
      LOG.debug("Deleted blobs in container {} for the remote log segment {}", containerName, remoteLogSegmentMetadata);
    } else {
      throw new AzureBlobStorageException(
          "Error deleting one or more blobs in container {} for the remote log segment {}",
          containerName, remoteLogSegmentMetadata);
    }
  }
}
