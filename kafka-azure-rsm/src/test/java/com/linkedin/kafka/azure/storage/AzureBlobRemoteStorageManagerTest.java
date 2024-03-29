/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.linkedin.kafka.azure.storage.cache.AzureBlobStorageCache;
import com.linkedin.kafka.azure.storage.cache.AzureBlobStorageMemoryCache;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.stream.Collectors;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
import static org.mockito.Mockito.*;

public class AzureBlobRemoteStorageManagerTest {
  private static final String AZURITE_BLOB_SERVICE_CONNECTION_STRING =
      String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;",
                    AZURITE_ACCOUNT_NAME, AZURITE_ACCOUNT_KEY, AZURITE_ENDPOINT, AZURITE_ACCOUNT_NAME);

  private static final File DIR = TieredStorageTestUtils.tempDirectory("azure-rsm-");
  private static final Random RANDOM = new Random();
  private static final Map<String, ?> AZURITE_CONFIG = TieredStorageTestUtils.getAzuriteConfig();

  private static Process azuriteProcess = null;

  private AzureBlobRemoteStorageManager remoteStorageManager;
  private BlobServiceClient serviceClient;

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
   */
  @BeforeAll
  public static void beforeClass() throws Exception {
    if (!isAzuriteListening()) {
      ProcessBuilder builder = new ProcessBuilder(Collections.singletonList("azurite-blob"));
      azuriteProcess = builder.inheritIO().start();
    }
  }

  @BeforeEach
  void beforeEach() {
    remoteStorageManager = new AzureBlobRemoteStorageManager();
    remoteStorageManager.configure(AZURITE_CONFIG);
    BlobServiceClientBuilder builder = new BlobServiceClientBuilder();
    serviceClient = builder.connectionString(AZURITE_BLOB_SERVICE_CONNECTION_STRING).buildClient();
    cleanUpAzurite();
  }

  private void cleanUpAzurite() {
    serviceClient.listBlobContainers().forEach(container ->
        serviceClient.getBlobContainerClient(container.getName()).delete());
  }

  @AfterEach
  void afterEach() throws IOException {
    remoteStorageManager.close();
    cleanUpAzurite();
  }

  /**
   * Cleanup and shutdown azurite-blob process started in {@link #beforeClass()}
   */
  @AfterAll
  public static void afterClass() {
    if (azuriteProcess != null) {
      List<ProcessHandle> children = azuriteProcess.children().collect(Collectors.toList());
      children.forEach(ProcessHandle::destroy);
      azuriteProcess.destroy();
    }
  }

  @Test
  public void testCopyLogSegmentSucceedsWhenContainerDoesNotExist() throws Exception {
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
  public void testCopyLogSegmentDoesNotFailIfContainerExists() throws Exception {
    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    LogSegmentData logSegmentData = createLogSegmentData();
    serviceClient.createBlobContainer(remoteStorageManager.getContainerName(segmentMetadata));

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
  public void testCopyLogSegmentDoesNotFailIfSegmentBlobExists() throws Exception {
    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    LogSegmentData logSegmentData = createLogSegmentData();
    String containerName = remoteStorageManager.getContainerName(segmentMetadata);
    serviceClient.createBlobContainer(containerName);
    BlobContainerClient blobContainerClient = serviceClient.getBlobContainerClient(containerName);

    BlobClient blobClient =
        blobContainerClient.getBlobClient(AzureBlobRemoteStorageManager.getBlobNameForSegment(segmentMetadata));
    BinaryData binaryData = BinaryData.fromBytes(TieredStorageTestUtils.randomBytes(100));
    blobClient.upload(binaryData);

    // Copy the segment, should not throw exception
    remoteStorageManager.copyLogSegmentData(segmentMetadata, logSegmentData);
  }

  private BlobStorageException buildMockBlobStorageException() {
    final BlobStorageException thrownException = mock(BlobStorageException.class);
    when(thrownException.getErrorCode()).thenReturn(BlobErrorCode.INTERNAL_ERROR);
    return thrownException;
  }

  @Test
  public void testCopyLogSegmentThrowsIfAzureBlobStorageThrowsOnSegmentCopy() throws Exception {
    AzureBlobStorageClient mockClient = mock(AzureBlobStorageClient.class);
    AzureBlobRemoteStorageManager rsmUnderTest = new AzureBlobRemoteStorageManager(
        new ContainerNameEncoder(""), mockClient, mock(AzureBlobStorageCache.class));

    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    LogSegmentData logSegmentData = createLogSegmentData();

    final BlobStorageException blobStorageException = buildMockBlobStorageException();
    when(mockClient.uploadFileToBlob(anyString(), anyString(), eq(logSegmentData.logSegment())))
        .thenThrow(blobStorageException);

    assertThrows(AzureBlobStorageException.class,
        () -> rsmUnderTest.copyLogSegmentData(segmentMetadata, logSegmentData));
  }

  @ParameterizedTest
  @EnumSource(value = RemoteStorageManager.IndexType.class, mode = EnumSource.Mode.EXCLUDE, names = "LEADER_EPOCH")
  void testCopyLogSegmentThrowsIfAzureBlobStorageThrowsOnIndexCopy(RemoteStorageManager.IndexType indexType) throws Exception {
    AzureBlobStorageClient mockClient = mock(AzureBlobStorageClient.class);
    AzureBlobRemoteStorageManager rsmUnderTest = new AzureBlobRemoteStorageManager(
        new ContainerNameEncoder(""), mockClient, mock(AzureBlobStorageCache.class));

    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    LogSegmentData logSegmentData = createLogSegmentData();

    final BlobStorageException blobStorageException = buildMockBlobStorageException();
    final Path expectedIndexFilePath = AzureBlobRemoteStorageManager.getIndexFilePath(logSegmentData, indexType).get();
    when(mockClient.uploadFileToBlob(anyString(), anyString(), eq(expectedIndexFilePath)))
        .thenThrow(blobStorageException);

    assertThrows(AzureBlobStorageException.class,
        () -> rsmUnderTest.copyLogSegmentData(segmentMetadata, logSegmentData));
  }

  @Test
  void testCopyLogSegmentThrowsIfAzureBlobStorageThrowsOnLeaderEpochIndexCopy() throws Exception {
    AzureBlobStorageClient mockClient = mock(AzureBlobStorageClient.class);
    AzureBlobRemoteStorageManager rsmUnderTest = new AzureBlobRemoteStorageManager(
        new ContainerNameEncoder(""), mockClient, mock(AzureBlobStorageCache.class));

    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    LogSegmentData logSegmentData = createLogSegmentData();

    final BlobStorageException blobStorageException = buildMockBlobStorageException();
    when(mockClient.uploadBytesToBlob(anyString(), anyString(), any(byte[].class)))
        .thenThrow(blobStorageException);

    assertThrows(AzureBlobStorageException.class,
        () -> rsmUnderTest.copyLogSegmentData(segmentMetadata, logSegmentData));
  }

  @ParameterizedTest
  @EnumSource(RemoteStorageManager.IndexType.class)
  public void testCopyLogSegmentDoesNotFailsIfIndexBlobExists(RemoteStorageManager.IndexType indexType) throws Exception {
    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    LogSegmentData logSegmentData = createLogSegmentData();
    String containerName = remoteStorageManager.getContainerName(segmentMetadata);
    serviceClient.createBlobContainer(containerName);
    BlobContainerClient blobContainerClient = serviceClient.getBlobContainerClient(containerName);

    BlobClient blobClient =
        blobContainerClient.getBlobClient(AzureBlobRemoteStorageManager.getBlobNameForIndex(
            segmentMetadata, indexType));
    BinaryData binaryData = BinaryData.fromBytes(TieredStorageTestUtils.randomBytes(100));
    blobClient.upload(binaryData);

    // Copy the segment, should not throw exception
    remoteStorageManager.copyLogSegmentData(segmentMetadata, logSegmentData);
  }

  @Test
  public void testCopyLogSegmentWithoutTxnIndex() throws Exception {
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

  @ParameterizedTest
  @EnumSource(value = RemoteStorageManager.IndexType.class)
  void testFetchLogSegmentIndexThrowsIfAzureBlobStorageThrows(RemoteStorageManager.IndexType indexType) {
    AzureBlobStorageClient mockClient = mock(AzureBlobStorageClient.class);
    AzureBlobStorageCache mockCache = mock(AzureBlobStorageCache.class);
    AzureBlobRemoteStorageManager rsmUnderTest = new AzureBlobRemoteStorageManager(
        new ContainerNameEncoder(""), mockClient, mockCache);

    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();

    final BlobStorageException blobStorageException = buildMockBlobStorageException();
    when(mockCache.getInputStream(anyString(),
        eq(AzureBlobRemoteStorageManager.getBlobNameForIndex(segmentMetadata, indexType))))
        .thenThrow(blobStorageException);

    assertThrows(AzureBlobStorageException.class,
        () -> rsmUnderTest.fetchIndex(segmentMetadata, indexType));
  }

  @Test
  void testFetchLogSegmentThrowsIfAzureBlobStorageThrows() {
    AzureBlobStorageClient mockClient = mock(AzureBlobStorageClient.class);
    AzureBlobStorageCache mockCache = mock(AzureBlobStorageCache.class);
    AzureBlobRemoteStorageManager rsmUnderTest = new AzureBlobRemoteStorageManager(
        new ContainerNameEncoder(""), mockClient, mockCache);

    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();

    final BlobStorageException blobStorageException = buildMockBlobStorageException();
    when(mockCache.getInputStream(anyString(),
        eq(AzureBlobRemoteStorageManager.getBlobNameForSegment(segmentMetadata)),
        anyInt(), anyInt()))
        .thenThrow(blobStorageException);

    assertThrows(AzureBlobStorageException.class,
        () -> rsmUnderTest.fetchLogSegment(segmentMetadata, 0));
  }

  @Test
  public void testFetchLogSegmentFailsIfContainerNotFound() {
    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();

    assertThrows(AzureResourceNotFoundException.class, () ->
        remoteStorageManager.fetchLogSegment(segmentMetadata, 0));
  }

  @Test
  public void testFetchLogSegmentFailsIfBlobNotFound() {
    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    String containerName = remoteStorageManager.getContainerName(segmentMetadata);
    serviceClient.createBlobContainer(containerName);

    assertThrows(AzureResourceNotFoundException.class, () ->
        remoteStorageManager.fetchLogSegment(segmentMetadata, 0));
  }

  @Test
  public void testFetchSegmentsForRange() throws Exception {
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
    assertThrows(AzureResourceNotFoundException.class, () -> remoteStorageManager.fetchLogSegment(segmentMetadata, 0));

    // Check that the segment data does not exist for range.
    assertThrows(AzureResourceNotFoundException.class, () -> remoteStorageManager.fetchLogSegment(segmentMetadata, 0, 1));

    // Check that all the indexes are not found.
    for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
      assertThrows(AzureResourceNotFoundException.class, () -> remoteStorageManager.fetchIndex(segmentMetadata, indexType));
    }
  }

  @Test
  public void testDeleteSegmentSucceedsIfContainerNotFound() throws Exception {
    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();

    // Create the container to avoid CONTAINER_NOT_FOUND error.
    serviceClient.createBlobContainer(remoteStorageManager.getContainerName(segmentMetadata));

    // Delete segment and check that it does not exist in RSM.
    remoteStorageManager.deleteLogSegmentData(segmentMetadata);
  }

  @Test
  public void testDeleteSegmentSucceedsIfBlobNotFound() throws Exception {
    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();

    // Delete segment and check that it does not exist in RSM.
    remoteStorageManager.deleteLogSegmentData(segmentMetadata);

    // Check that the segment data does not exist.
    assertThrows(AzureResourceNotFoundException.class, () -> remoteStorageManager.fetchLogSegment(segmentMetadata, 0));

    // Check that the segment data does not exist for range.
    assertThrows(AzureResourceNotFoundException.class, () -> remoteStorageManager.fetchLogSegment(segmentMetadata, 0, 1));

    // Check that all the indexes are not found.
    for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
      assertThrows(AzureResourceNotFoundException.class, () -> remoteStorageManager.fetchIndex(segmentMetadata, indexType));
    }
  }

  @Test
  public void testDeleteSegmentThrowsIfAzureBlobStorageThrows() {
    AzureBlobStorageClient mockClient = mock(AzureBlobStorageClient.class);
    AzureBlobStorageCache mockCache = mock(AzureBlobStorageCache.class);
    AzureBlobRemoteStorageManager rsmUnderTest = new AzureBlobRemoteStorageManager(
        new ContainerNameEncoder(""), mockClient, mockCache);

    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();

    final BlobStorageException blobStorageException = buildMockBlobStorageException();

    doThrow(blobStorageException)
        .doNothing()
        .when(mockClient)
        .deleteBlob(anyString(), eq(AzureBlobRemoteStorageManager.getBlobNameForSegment(segmentMetadata)));

    assertThrows(AzureBlobStorageException.class, () -> rsmUnderTest.deleteLogSegmentData(segmentMetadata));

    verify(mockClient).deleteBlob(anyString(),
        eq(AzureBlobRemoteStorageManager.getBlobNameForSegment(segmentMetadata)));

    // Verify that even though the deleteBlob call for the segment blob fails,
    // deleteBlob is subsequently invoked for index blobs.
    for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
      verify(mockClient).deleteBlob(anyString(),
          eq(AzureBlobRemoteStorageManager.getBlobNameForIndex(segmentMetadata, indexType)));
    }
  }

  @Test
  public void testDeletingOneSegmentDoesNotDeleteOtherSegments() throws Exception {
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
    assertThrows(AzureResourceNotFoundException.class, () -> remoteStorageManager.fetchLogSegment(segment1Metadata, 0));

    // Check that the segment data does not exist for range.
    assertThrows(AzureResourceNotFoundException.class, () -> remoteStorageManager.fetchLogSegment(segment1Metadata, 0, 1));

    // Check that all the indexes are not found.
    for (RemoteStorageManager.IndexType indexType : RemoteStorageManager.IndexType.values()) {
      assertThrows(AzureResourceNotFoundException.class, () -> remoteStorageManager.fetchIndex(segment1Metadata, indexType));
    }

    // Check that the copied second segment exists in rsm and it is same.
    try (InputStream segmentStream = remoteStorageManager.fetchLogSegment(segment2Metadata, 0)) {
      matchBytes(segmentStream, logSegment2Data.logSegment());
    }
  }

  @Test
  void testFetchedLogSegmentIsCached() throws Exception {
    AzureBlobStorageClient mockAzureClient = mock(AzureBlobStorageClient.class);
    AzureBlobRemoteStorageManager rsmUnderTest = new AzureBlobRemoteStorageManager(
        new ContainerNameEncoder(""), mockAzureClient,
        new AzureBlobStorageMemoryCache(1024, mockAzureClient::fetchBlobAsBytes));

    RemoteLogSegmentMetadata segmentMetadata = createRemoteLogSegmentMetadata();
    LogSegmentData logSegmentData = createLogSegmentData();
    rsmUnderTest.copyLogSegmentData(segmentMetadata, logSegmentData);

    verify(mockAzureClient, atLeastOnce()).uploadFileToBlob(anyString(), anyString(), any(Path.class));
    verify(mockAzureClient, atLeastOnce()).uploadBytesToBlob(anyString(), anyString(), any(byte[].class));

    when(mockAzureClient.fetchBlobAsBytes(anyString(), anyString()))
        .thenReturn(Files.readAllBytes(logSegmentData.logSegment()));

    final InputStream initialSegmentStream = rsmUnderTest.fetchLogSegment(segmentMetadata, 0);
    matchBytes(initialSegmentStream, logSegmentData.logSegment());

    verify(mockAzureClient).fetchBlobAsBytes(anyString(), anyString());

    // Replace Azure client with mock client

    // Now verify that the mock client never gets called again, but cached segment is returned
    final InputStream cachedSegmentStream = rsmUnderTest.fetchLogSegment(segmentMetadata, 0);
    matchBytes(cachedSegmentStream, logSegmentData.logSegment());

    verifyNoMoreInteractions(mockAzureClient);
  }
}
