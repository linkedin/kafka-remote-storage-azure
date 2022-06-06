/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
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
  private BlobServiceClient blobServiceClient;
  private final MetricRegistry metricRegistry = new MetricRegistry();
  private final Timer writeLatencyTimer = metricRegistry.timer(WRITE_LATENCY_MILLIS);
  private final Meter bytesOutRateMeter = metricRegistry.meter(BYTES_OUT_RATE);
  private AzureBlobRemoteStorageConfig config;

  @Override
  public void close() {
    LOG.info("Shutting down {}", this.getClass().getSimpleName());
  }

  @Override
  public void configure(Map<String, ?> configs) {
    LOG.info("Setting up {} with configs: ", this.getClass().getSimpleName());
    for (Map.Entry<String, ?> entry: configs.entrySet()) {
      LOG.info("{}: {} ", entry.getKey(), entry.getValue());
    }
    config = new AzureBlobRemoteStorageConfig(configs);
    containerNameEncoder = new ContainerNameEncoder(config.getContainerNamePrefix());
    blobServiceClient = BlobServiceClientBuilderFactory.getBlobServiceClientBuilder(config).buildClient();

    ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry).build();
    reporter.start(600, TimeUnit.SECONDS);
    reporter.report();
  }

  private int uploadToAzureBlob(final BlobContainerClient blobContainerClient, final String blobName, byte[] data)
      throws RemoteStorageException {
    BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
    blobClient.upload(BinaryData.fromBytes(data), true);
    return data.length;
  }

  private long uploadToAzureBlob(final BlobContainerClient blobContainerClient, final String blobName, Path inputFile)
      throws IOException, RemoteStorageException {
    BlobClient blobClient = blobContainerClient.getBlobClient(blobName);

    ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions();
    parallelTransferOptions.setMaxSingleUploadSizeLong(config.getMaxSingleUploadSize());
    parallelTransferOptions.setBlockSizeLong(config.getBlockSize());
    blobClient.uploadFromFile(inputFile.toString(), null, null, null, null, null, null);
    return Files.size(inputFile);
  }

  private byte[] fetchBlob(final BlobContainerClient blobContainerClient, final String fileName) throws RemoteResourceNotFoundException {
    BlobClient blobClient = blobContainerClient.getBlobClient(fileName);

    try {
      long dataSize = blobClient.getProperties().getBlobSize();
      if (dataSize > (long) Integer.MAX_VALUE) {
        String msg = String.format("Block blob %s in container %s has size %d that is larger than Integer.MAX_VALUE",
            fileName, blobContainerClient.getBlobContainerName(), dataSize);
        throw new IllegalStateException(msg);
      }

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream((int) dataSize);
      blobClient.downloadStream(outputStream);
      return outputStream.toByteArray();
    } catch (BlobStorageException error) {
      if (error.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
        String msg = String.format(
            "Block blob %s does not exist in container %s", fileName, blobContainerClient.getBlobContainerName());
        throw new RemoteResourceNotFoundException(msg);
      } else if (error.getErrorCode().equals(BlobErrorCode.CONTAINER_NOT_FOUND)) {
        String msg = String.format("Container %s does not exist.", blobContainerClient.getBlobContainerName());
        throw new RemoteResourceNotFoundException(msg);
      } else {
        throw error;
      }
    }
  }

  private BlobContainerClient getContainerClient(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
    String containerName = getContainerName(remoteLogSegmentMetadata);
    return blobServiceClient.getBlobContainerClient(containerName);
  }

  /* Visible only for testing */
  boolean containsFile(RemoteLogSegmentMetadata remoteLogSegmentMetadata, String fileName) {
    BlobContainerClient blobContainerClient = getContainerClient(remoteLogSegmentMetadata);
    BlobClient blobClient = blobContainerClient.getBlobClient(fileName);
    return blobClient.exists();
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

  public String getContainerName(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
    return containerNameEncoder.encode(remoteLogSegmentMetadata);
  }

  @Override
  public void copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                 LogSegmentData logSegmentData) throws RemoteStorageException {
    if (remoteLogSegmentMetadata == null) {
      throw new NullPointerException("remoteLogSegmentMetadata must be non-null");
    }

    Timer.Context timeContext = writeLatencyTimer.time();
    try {
      attemptCopyLogSegmentData(remoteLogSegmentMetadata, logSegmentData);
    } catch (BlobStorageException error) {
      if (error.getErrorCode().equals(BlobErrorCode.CONTAINER_NOT_FOUND)) {
        createContainerForRemoteLogSegment(remoteLogSegmentMetadata);
        attemptCopyLogSegmentData(remoteLogSegmentMetadata, logSegmentData);
      } else {
        throw error;
      }
    } catch (RemoteStorageException e) {
      throw e;
    } catch (Exception e) {
      throw new RemoteStorageException(e);
    } finally {
      timeContext.stop();
    }
  }

  private void createContainerForRemoteLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata)
      throws RemoteStorageException {
    BlobContainerClient blobContainerClient = getContainerClient(remoteLogSegmentMetadata);

    final String blobContainerName = blobContainerClient.getBlobContainerName();
    try {
      LOG.info("Container {} not found for remote log segment {}. Creating container.",
          blobContainerName, remoteLogSegmentMetadata.remoteLogSegmentId());
      blobContainerClient.create();
    } catch (BlobStorageException error) {
      // Because of multiple clients trying to create the container at the same time, it
      // is possible that the container already exists by the time we try to create it.
      if (!error.getErrorCode().equals(BlobErrorCode.CONTAINER_ALREADY_EXISTS)) {
        throw new RemoteStorageException(error);
      }
    } catch (Exception e) {
      throw new RemoteStorageException(e);
    }
  }

  private void attemptCopyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
      LogSegmentData logSegmentData) throws RemoteStorageException {
    LOG.debug("Writing remote log segment started for {}", remoteLogSegmentMetadata);

    if (remoteLogSegmentMetadata == null) {
      throw new NullPointerException("remoteLogSegmentMetadata must be non-null");
    }
    if (logSegmentData == null) {
      throw new NullPointerException(String.format("logSegmentData must be non-null for remote log segment %s", remoteLogSegmentMetadata));
    }
    BlobContainerClient blobContainerClient = getContainerClient(remoteLogSegmentMetadata);

    final String segmentKey = getBlobNameForSegment(remoteLogSegmentMetadata);
    try {
      long segmentBytes = uploadToAzureBlob(blobContainerClient, segmentKey, logSegmentData.logSegment());
      long leaderEpochIndexBytes = uploadToAzureBlob(blobContainerClient,
                                                    getBlobNameForIndex(remoteLogSegmentMetadata, IndexType.LEADER_EPOCH),
                                                    logSegmentData.leaderEpochIndex().array());
      long producerSnapshotBytes = uploadToAzureBlob(blobContainerClient,
                                              getBlobNameForIndex(remoteLogSegmentMetadata, IndexType.PRODUCER_SNAPSHOT),
                                              logSegmentData.producerSnapshotIndex());
      long offsetIndexBytes = uploadToAzureBlob(blobContainerClient,
                                               getBlobNameForIndex(remoteLogSegmentMetadata, IndexType.OFFSET),
                                               logSegmentData.offsetIndex());
      long timeIndexBytes = uploadToAzureBlob(blobContainerClient,
                                             getBlobNameForIndex(remoteLogSegmentMetadata, IndexType.TIMESTAMP),
                                             logSegmentData.timeIndex());

      if (logSegmentData.transactionIndex().isPresent()) {
        uploadToAzureBlob(blobContainerClient, getBlobNameForIndex(remoteLogSegmentMetadata, IndexType.TRANSACTION),
            logSegmentData.transactionIndex().get());
      }

      LOG.debug("Writing remote log segment completed for {}", remoteLogSegmentMetadata);
      bytesOutRateMeter.mark(segmentBytes + leaderEpochIndexBytes + producerSnapshotBytes + offsetIndexBytes + timeIndexBytes);
    } catch (IOException e) {
      throw new RemoteStorageException(e);
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

    String segmentKey = getBlobNameForSegment(remoteLogSegmentMetadata);
    BlobContainerClient blobContainerClient = getContainerClient(remoteLogSegmentMetadata);
    byte[] segmentBytes;
    // Read the entire log segment
    segmentBytes = fetchBlob(blobContainerClient, segmentKey);
    LOG.debug("Fetched remote log segment {} of length {}", remoteLogSegmentMetadata, segmentBytes.length);

    if (startPosition >= segmentBytes.length) {
      throw new IllegalArgumentException(
          String.format("start position %d must be less than segment length %d in remote log segment %s",
                        startPosition, segmentBytes.length, remoteLogSegmentMetadata));
    }

    // The number of bytes to read from the segment must not be more than the size of the segment (duh!)
    // The interface post-condition requires that the returned InputStream will end at the smaller of
    // endPosition and the end of the remote log segment data file/object. Note, endPosition is inclusive.
    // Therefore, minimum of segmentBytes.length-1 (which is the highest possible byte offset that can be
    // returned) and the input endPosition is taken below.
    int length = Math.min(segmentBytes.length - 1, endPosition) - startPosition + 1;

    return new ByteArrayInputStream(segmentBytes, startPosition, length);
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

    String indexKey = getBlobNameForIndex(remoteLogSegmentMetadata, indexType);
    BlobContainerClient blobContainerClient = getContainerClient(remoteLogSegmentMetadata);
    byte[] index = fetchBlob(blobContainerClient, indexKey);

    if (index == null) {
      String msg = String.format("Non-existent remote log segment %s index with start offset %d and id %s.", indexType,
                                 remoteLogSegmentMetadata.startOffset(), remoteLogSegmentMetadata.remoteLogSegmentId());
      throw new RemoteResourceNotFoundException(msg);
    }

    return new ByteArrayInputStream(index);
  }

  private void deleteBlob(RemoteLogSegmentMetadata remoteLogSegmentMetadata, BlobContainerClient blobContainerClient, String blobName)
      throws RemoteStorageException {
    final String containerName = blobContainerClient.getBlobContainerName();
    try {
      blobContainerClient.getBlobClient(blobName).delete();
    } catch (BlobStorageException ex) {
      LOG.error("Failure during deletion of blob {} in container {} for remote log segment {}. Status Code = {}, Error code = {}",
                blobName, containerName, remoteLogSegmentMetadata, ex.getStatusCode(), ex.getErrorCode());
      throw new RemoteStorageException(ex);
    }
  }

  @Override
  public void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
    Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

    BlobContainerClient blobContainerClient = getContainerClient(remoteLogSegmentMetadata);
    final String containerName = blobContainerClient.getBlobContainerName();
    LOG.debug("Started deletion of blobs in container {} for the remote log segment {}", containerName, remoteLogSegmentMetadata);
    deleteBlob(remoteLogSegmentMetadata, blobContainerClient, getBlobNameForSegment(remoteLogSegmentMetadata));

    for (IndexType indexType: IndexType.values()) {
      String indexBlob = getBlobNameForIndex(remoteLogSegmentMetadata, indexType);

      // In linkedin we don't use Kafka transactions but there should be a better way
      // to detect if the transaction index was ever created or not.
      // Perhaps RemoteLogSegmentMetadata should include a byte[] for arbitrary data.
      if (indexType == IndexType.TRANSACTION) {
        BlobClient blobClient = blobContainerClient.getBlobClient(indexBlob);
        if (blobClient.exists()) {
          deleteBlob(remoteLogSegmentMetadata, blobContainerClient, getBlobNameForIndex(remoteLogSegmentMetadata, indexType));
        }
      } else {
        // Delete blobs other than txnIndex unconditionally because we expect them to be present modulo copy errors.
        deleteBlob(remoteLogSegmentMetadata, blobContainerClient, getBlobNameForIndex(remoteLogSegmentMetadata, indexType));
      }
    }

    LOG.debug("Deleted blobs in container {} for the remote log segment {}", containerName, remoteLogSegmentMetadata);
  }
}
