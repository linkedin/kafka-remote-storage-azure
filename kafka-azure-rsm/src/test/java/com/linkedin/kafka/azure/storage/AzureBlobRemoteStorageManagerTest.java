/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.stream.Collectors;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static com.linkedin.kafka.azure.storage.RemoteStorageManagerDefaults.*;
import static org.junit.jupiter.api.Assertions.*;

public class AzureBlobRemoteStorageManagerTest {
  private static final Logger log = LoggerFactory.getLogger(AzureBlobRemoteStorageManagerTest.class);

  private static final String AZURITE_BLOB_SERVICE_CONNECTION_STRING =
      String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;",
                    AZURITE_ACCOUNT_NAME, AZURITE_ACCOUNT_KEY, AZURITE_ENDPOINT, AZURITE_ACCOUNT_NAME);

  private static final File DIR = TieredStorageTestUtils.tempDirectory("azure-rsm-");
  private static final Random RANDOM = new Random();
  private static final Map<String, ?> AZURITE_CONFIG = TieredStorageTestUtils.getAzuriteConfig();

  private static Process azuriteProcess = null;

  private static void cleanUpAzurite() {
    BlobServiceClientBuilder builder = new BlobServiceClientBuilder();
    BlobServiceClient serviceClient = builder.connectionString(AZURITE_BLOB_SERVICE_CONNECTION_STRING).buildClient();
    serviceClient.listBlobContainers().forEach(container -> {
      serviceClient.getBlobContainerClient(container.getName()).delete();
    });
  }

  private static RemoteLogSegmentMetadata createRemoteLogSegmentMetadata() {
    return TieredStorageTestUtils.createRemoteLogSegmentMetadata("foo", 1);
  }

  private static void matchBytes(InputStream segmentStream, Path path) throws IOException {
    byte[] segmentBytes = Files.readAllBytes(path);
    ByteBuffer byteBuffer = readAsByteBuffer(segmentStream, segmentBytes.length);
    assertEquals(ByteBuffer.wrap(segmentBytes), byteBuffer);
  }

  private static ByteBuffer readAsByteBuffer(InputStream segmentStream, int len) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[len]);
    Utils.readFully(segmentStream, byteBuffer);
    byteBuffer.rewind();
    return byteBuffer;
  }

  private static LogSegmentData createLogSegmentDataWithoutTxnIndex() throws Exception {
    return createLogSegmentData(100, false);
  }

  private static LogSegmentData createLogSegmentData() throws Exception {
    return createLogSegmentData(100, true);
  }

  private static LogSegmentData createLogSegmentData(int segmentSize, boolean withTxnIndex) throws Exception {
    int prefix = Math.abs(RANDOM.nextInt());
    Path segment = new File(DIR, prefix + ".seg").toPath();
    Files.write(segment, TieredStorageTestUtils.randomBytes(segmentSize));

    Path offsetIndex = new File(DIR, prefix + ".oi").toPath();
    Files.write(offsetIndex, TieredStorageTestUtils.randomBytes(10));

    Path timeIndex = new File(DIR, prefix + ".ti").toPath();
    Files.write(timeIndex, TieredStorageTestUtils.randomBytes(10));

    Optional<Path> txnIndexOp = Optional.empty();
    if (withTxnIndex) {
      txnIndexOp = Optional.of(new File(DIR, prefix + ".txni").toPath());
      Files.write(txnIndexOp.get(), TieredStorageTestUtils.randomBytes(10));
    }

    Path producerSnapshotIndex = new File(DIR, prefix + ".psi").toPath();
    Files.write(producerSnapshotIndex, TieredStorageTestUtils.randomBytes(10));

    ByteBuffer leaderEpochIndex = ByteBuffer.wrap(TieredStorageTestUtils.randomBytes(10));
    return new LogSegmentData(segment, offsetIndex, timeIndex, txnIndexOp, producerSnapshotIndex, leaderEpochIndex);
  }

  private static void doTestFetchForRange(RemoteStorageManager remoteStorageManager, RemoteLogSegmentMetadata remoteLogSegmentMetadata, Path path,
                                   int startPosition, int length) throws Exception {
    // Read from the segment for the expected range.
    ByteBuffer expectedSegmentRangeBytes = ByteBuffer.allocate(length);
    try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(path)) {
      seekableByteChannel.position(startPosition).read(expectedSegmentRangeBytes);
    }
    expectedSegmentRangeBytes.rewind();

    // Fetch from in-memory RSM for the same range
    ByteBuffer fetchedSegRangeBytes = ByteBuffer.allocate(length);
    try (InputStream segmentRangeStream = remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata, startPosition, startPosition + length - 1)) {
      Utils.readFully(segmentRangeStream, fetchedSegRangeBytes);
    }
    fetchedSegRangeBytes.rewind();
    assertEquals(expectedSegmentRangeBytes, fetchedSegRangeBytes);
  }

  private static boolean isAzuriteListening() {
    Endpoint endpoint = Endpoint.fromString(AZURITE_ENDPOINT);
    SocketAddress socketAddress = new InetSocketAddress(endpoint.host(), Integer.parseInt(endpoint.port().get()));
    Socket socket = new Socket();
    int timeout = 500;

    try {
      socket.connect(socketAddress, timeout);
      socket.close();
      return true;
    } catch (Exception exception) {
      return false;
    }
  }

  /**
   * Start azurite-blob process if not already running
   * @throws Exception
   */
  @BeforeAll
  public static void beforeClass() throws Exception {
    if (!isAzuriteListening()) {
      ProcessBuilder builder = new ProcessBuilder(Collections.singletonList("azurite-blob"));
      azuriteProcess = builder.inheritIO().start();
    }
  }

  /**
   * Cleanup and shutdown azurite-blob process started in {@link #beforeClass()}
   */
  @AfterAll
  public static void afterClass() {
    if (azuriteProcess != null) {
      try {
        cleanUpAzurite();
      } finally {
        List<ProcessHandle> children = azuriteProcess.children().collect(Collectors.toList());
        children.forEach(ProcessHandle::destroy);
        azuriteProcess.destroy();
      }
    }
  }

  @Test
  public void testCopyLogSegment() throws Exception {
    AzureBlobRemoteStorageManager remoteStorageManager = new AzureBlobRemoteStorageManager();
    remoteStorageManager.configure(AZURITE_CONFIG);

    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    LogSegmentData logSegmentData = createLogSegmentData();

    // Copy the segment
    remoteStorageManager.copyLogSegmentData(segmentMetadata, logSegmentData);

    // Check that the segment exists in the RSM.
    boolean containsSegment =
        remoteStorageManager.containsFile(segmentMetadata, AzureBlobRemoteStorageManager.getBlobNameForSegment(segmentMetadata));
    assertTrue(containsSegment);

    // Check that the indexes exist in the RSM.
    for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
      boolean containsIndex =
          remoteStorageManager.containsFile(segmentMetadata, AzureBlobRemoteStorageManager.getBlobNameForIndex(segmentMetadata, indexType));
      assertTrue(containsIndex);
    }
  }

  @Test
  public void testCopyLogSegmentWithoutTxnIndex() throws Exception {
    AzureBlobRemoteStorageManager remoteStorageManager = new AzureBlobRemoteStorageManager();
    remoteStorageManager.configure(AZURITE_CONFIG);

    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    LogSegmentData logSegmentData = createLogSegmentDataWithoutTxnIndex();

    // Copy the segment
    remoteStorageManager.copyLogSegmentData(segmentMetadata, logSegmentData);

    // Check that the segment exists in the RSM.
    boolean containsSegment =
        remoteStorageManager.containsFile(segmentMetadata, AzureBlobRemoteStorageManager.getBlobNameForSegment(segmentMetadata));
    assertTrue(containsSegment);

    // Check that the indexes exist in the RSM.
    for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
      if (indexType == RemoteStorageManager.IndexType.TRANSACTION) {
        continue;
      }
      boolean containsIndex =
          remoteStorageManager.containsFile(segmentMetadata, AzureBlobRemoteStorageManager.getBlobNameForIndex(segmentMetadata, indexType));
      assertTrue(containsIndex);
    }
  }

  @Test
  public void testFetchLogSegmentIndexes() throws Exception {
    RemoteStorageManager remoteStorageManager = new AzureBlobRemoteStorageManager();
    remoteStorageManager.configure(AZURITE_CONFIG);

    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    LogSegmentData logSegmentData = createLogSegmentData();

    // Copy the segment
    remoteStorageManager.copyLogSegmentData(segmentMetadata, logSegmentData);

    // Check segment data exists for the copied segment.
    try (InputStream segmentStream = remoteStorageManager.fetchLogSegment(segmentMetadata, 0)) {
      matchBytes(segmentStream, logSegmentData.logSegment());
    }

    HashMap<RemoteStorageManager.IndexType, Path> expectedIndexToPaths = new HashMap<>();
    expectedIndexToPaths.put(RemoteStorageManager.IndexType.OFFSET, logSegmentData.offsetIndex());
    expectedIndexToPaths.put(RemoteStorageManager.IndexType.TIMESTAMP, logSegmentData.timeIndex());
    expectedIndexToPaths.put(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT, logSegmentData.producerSnapshotIndex());

    logSegmentData.transactionIndex().ifPresent(txnIndex -> expectedIndexToPaths.put(RemoteStorageManager.IndexType.TRANSACTION, txnIndex));

    // Check all segment indexes exist for the copied segment.
    for (Map.Entry<RemoteStorageManager.IndexType, Path> entry : expectedIndexToPaths.entrySet()) {
      RemoteStorageManager.IndexType indexType = entry.getKey();
      Path indexPath = entry.getValue();

      try (InputStream offsetIndexStream = remoteStorageManager.fetchIndex(segmentMetadata, indexType)) {
        matchBytes(offsetIndexStream, indexPath);
      }
    }

    try (InputStream leaderEpochIndexStream = remoteStorageManager.fetchIndex(segmentMetadata, RemoteStorageManager.IndexType.LEADER_EPOCH)) {
      ByteBuffer leaderEpochIndex = logSegmentData.leaderEpochIndex();
      assertEquals(leaderEpochIndex, readAsByteBuffer(leaderEpochIndexStream, leaderEpochIndex.array().length));
    }
  }

  @Test
  public void testFetchSegmentsForRange() throws Exception {
    RemoteStorageManager remoteStorageManager = new AzureBlobRemoteStorageManager();
    remoteStorageManager.configure(AZURITE_CONFIG);

    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    int segmentSize = 100;
    LogSegmentData logSegmentData = createLogSegmentData(segmentSize, true);
    Path path = logSegmentData.logSegment();

    // Copy the segment
    remoteStorageManager.copyLogSegmentData(segmentMetadata, logSegmentData);

    // 1. Fetch segment for startPos at 0
    doTestFetchForRange(remoteStorageManager, segmentMetadata, path, 0, 40);

    // 2. Fetch segment for start and end positions as start and end of the segment.
    doTestFetchForRange(remoteStorageManager, segmentMetadata, path, 0, segmentSize);

    // 3. Fetch segment for endPos at the end of segment.
    doTestFetchForRange(remoteStorageManager, segmentMetadata, path, 90, segmentSize - 90);

    // 4. Fetch segment only for the start position.
    doTestFetchForRange(remoteStorageManager, segmentMetadata, path, 0, 1);

    // 5. Fetch segment only for the end position.
    doTestFetchForRange(remoteStorageManager, segmentMetadata, path, segmentSize - 1, 1);

    // 6. Fetch for any range other than boundaries.
    doTestFetchForRange(remoteStorageManager, segmentMetadata, path, 3, 90);
  }

  @Test
  public void testFetchInvalidRange() throws Exception {
    RemoteStorageManager remoteStorageManager = new AzureBlobRemoteStorageManager();
    remoteStorageManager.configure(AZURITE_CONFIG);

    RemoteLogSegmentMetadata remoteLogSegmentMetadata = createRemoteLogSegmentMetadata();
    LogSegmentData logSegmentData = createLogSegmentData();

    // Copy the segment
    remoteStorageManager.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData);

    // Check fetch segments with invalid ranges like startPos < endPos
    assertThrows(Exception.class, () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata, 2, 1));

    // Check fetch segments with invalid ranges like startPos or endPos as negative.
    assertThrows(Exception.class, () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata, -1, 0));
    assertThrows(Exception.class, () -> remoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata, -2, -1));
  }

  @Test
  public void testDeleteSegment() throws Exception {
    RemoteStorageManager remoteStorageManager = new AzureBlobRemoteStorageManager();
    remoteStorageManager.configure(AZURITE_CONFIG);

    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    LogSegmentData logSegmentData = createLogSegmentData();

    // Copy a log segment.
    remoteStorageManager.copyLogSegmentData(segmentMetadata, logSegmentData);

    // Check that the copied segment exists in rsm and it is same.
    try (InputStream segmentStream = remoteStorageManager.fetchLogSegment(segmentMetadata, 0)) {
      matchBytes(segmentStream, logSegmentData.logSegment());
    }

    // Delete segment and check that it does not exist in RSM.
    remoteStorageManager.deleteLogSegmentData(segmentMetadata);

    // Check that the segment data does not exist.
    assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorageManager.fetchLogSegment(segmentMetadata, 0));

    // Check that the segment data does not exist for range.
    assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorageManager.fetchLogSegment(segmentMetadata, 0, 1));

    // Check that all the indexes are not found.
    for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
      assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorageManager.fetchIndex(segmentMetadata, indexType));
    }
  }

  @Test
  public void testDeletingOneSegmentDoesNotDeleteOtherSegments() throws Exception {
    RemoteStorageManager remoteStorageManager = new AzureBlobRemoteStorageManager();
    remoteStorageManager.configure(AZURITE_CONFIG);

    final Uuid topicId = Uuid.randomUuid();
    final int leaderEpochStartOffset = 0;
    RemoteLogSegmentMetadata segment1Metadata =
        TieredStorageTestUtils.createRemoteLogSegmentMetadata("foo", 0, topicId, 0, 99, leaderEpochStartOffset);
    LogSegmentData logSegment1Data = createLogSegmentData();

    // Copy the first log segment.
    remoteStorageManager.copyLogSegmentData(segment1Metadata, logSegment1Data);

    RemoteLogSegmentMetadata segment2Metadata =
        TieredStorageTestUtils.createRemoteLogSegmentMetadata("foo", 0, topicId, 100, 199, leaderEpochStartOffset);
    LogSegmentData logSegment2Data = createLogSegmentData();
    // Copy the second log segment.
    remoteStorageManager.copyLogSegmentData(segment2Metadata, logSegment2Data);

    // Check that the copied first segment exists in rsm and it is same.
    try (InputStream segmentStream = remoteStorageManager.fetchLogSegment(segment1Metadata, 0)) {
      matchBytes(segmentStream, logSegment1Data.logSegment());
    }

    // Check that the copied second segment exists in rsm and it is same.
    try (InputStream segmentStream = remoteStorageManager.fetchLogSegment(segment2Metadata, 0)) {
      matchBytes(segmentStream, logSegment2Data.logSegment());
    }

    // Delete the first segment and check that it does not exist in RSM.
    remoteStorageManager.deleteLogSegmentData(segment1Metadata);

    // Check that the segment data does not exist.
    assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorageManager.fetchLogSegment(segment1Metadata, 0));

    // Check that the segment data does not exist for range.
    assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorageManager.fetchLogSegment(segment1Metadata, 0, 1));

    // Check that all the indexes are not found.
    for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
      assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorageManager.fetchIndex(segment1Metadata, indexType));
    }

    // Check that the copied second segment exists in rsm and it is same.
    try (InputStream segmentStream = remoteStorageManager.fetchLogSegment(segment2Metadata, 0)) {
      matchBytes(segmentStream, logSegment2Data.logSegment());
    }
  }
}
