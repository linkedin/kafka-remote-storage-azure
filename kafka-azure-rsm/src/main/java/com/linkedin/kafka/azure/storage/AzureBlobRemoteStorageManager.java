/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.io.BufferedInputStream;
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
    containerNameEncoder = new ContainerNameEncoder(RemoteStorageManagerDefaults.getOrDefaultContainerNamePrefix(configs));
    blobServiceClient = BlobServiceClientBuilderFactory.getBlobServiceClientBuilder(configs).buildClient();

    ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry).build();
    reporter.start(600, TimeUnit.SECONDS);
    reporter.report();
  }

  private int uploadToAzureBlob(final BlobContainerClient blobContainerClient, final String fileName, byte[] data) throws IOException {
    BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(fileName).getBlockBlobClient();
    try (InputStream inputStream = new ByteArrayInputStream(data)) {
      blockBlobClient.upload(inputStream, data.length, true);
    }
    return data.length;
  }

  private long uploadToAzureBlob(final BlobContainerClient blobContainerClient, final String fileName, Path inputFile) throws IOException {
    long size = Files.size(inputFile);
    BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(fileName).getBlockBlobClient();
    try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(inputFile))) {
      blockBlobClient.upload(inputStream, Files.size(inputFile), true);
    }
    return size;
  }

  private byte[] fetchBlob(final BlobContainerClient blobContainerClient, final String fileName) throws RemoteResourceNotFoundException {
    BlockBlobClient blockBlobClient = null;

    blockBlobClient = blobContainerClient.getBlobClient(fileName).getBlockBlobClient();
    if (!blockBlobClient.exists()) {
      String msg = String.format("Block blob %s does not exist in container %s", fileName, blobContainerClient.getBlobContainerName());
      throw new RemoteResourceNotFoundException(msg);
    }

    long dataSize = blockBlobClient.getProperties().getBlobSize();
    if (dataSize > (long) Integer.MAX_VALUE) {
      String msg = String.format("Block blob %s in container %s has size %d that is larger than Integer.MAX_VALUE",
                                 fileName, blobContainerClient.getBlobContainerName(), dataSize);
      throw new IllegalStateException(msg);
    }

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream((int) dataSize);
    blockBlobClient.downloadStream(outputStream);
    return outputStream.toByteArray();
  }

  private BlobContainerClient getContainerClient(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
    String containerName = getContainerName(remoteLogSegmentMetadata);
    return blobServiceClient.getBlobContainerClient(containerName);
  }

  boolean containsFile(RemoteLogSegmentMetadata remoteLogSegmentMetadata, String fileName) {
    BlobContainerClient blobContainerClient = getContainerClient(remoteLogSegmentMetadata);
    BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(fileName).getBlockBlobClient();
    return blockBlobClient.exists();
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
    LOG.debug("Writing remote log segment started for {}", remoteLogSegmentMetadata);

    if (remoteLogSegmentMetadata == null) {
      throw new NullPointerException("remoteLogSegmentMetadata must be non-null");
    }
    if (logSegmentData == null) {
      throw new NullPointerException(String.format("logSegmentData must be non-null for remote log segment %s", remoteLogSegmentMetadata));
    }
    BlobContainerClient blobContainerClient = getContainerClient(remoteLogSegmentMetadata);

    final String blobContainerName = blobContainerClient.getBlobContainerName();
    if (!blobContainerClient.exists()) {
      LOG.debug("Creating container {}", blobContainerName);
      blobContainerClient.create();
    } else {
      LOG.debug("Container already exists {}", blobContainerName);
    }

    final String segmentKey = getBlobNameForSegment(remoteLogSegmentMetadata);
    BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(segmentKey).getBlockBlobClient();

    if (blockBlobClient.exists()) {
      throw new RemoteStorageException(String.format("Blob container %s already contains the segment for id %s",
                                                     blobContainerName, remoteLogSegmentMetadata.remoteLogSegmentId()));
    }

    Timer.Context timeContext = writeLatencyTimer.time();
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
        uploadToAzureBlob(blobContainerClient,
                          getBlobNameForIndex(remoteLogSegmentMetadata, IndexType.TRANSACTION),
                          logSegmentData.transactionIndex().get());
      }

      LOG.debug("Writing remote log segment completed for {}", remoteLogSegmentMetadata);
      bytesOutRateMeter.mark(segmentBytes + leaderEpochIndexBytes + producerSnapshotBytes + offsetIndexBytes + timeIndexBytes);
    } catch (Exception e) {
      throw new RemoteStorageException(e);
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