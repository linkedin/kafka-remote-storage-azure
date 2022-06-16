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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class AzureBlobStorageClientTest {
  private static final String TEST_CONTAINER_1 = "test-container-1";
  private static final String TEST_BLOB_1 = "test-blob-1";
  private static final Path TEST_PATH_1 = Paths.get("test-file-1");

  @Mock
  private BlobServiceClient mockServiceClient;
  @Mock
  private BlobContainerClient mockContainerClient;
  @Mock
  private BlobClient mockBlobClient;

  private AzureBlobStorageClient client;

  @BeforeEach
  void setup() {
    when(mockServiceClient.getBlobContainerClient(anyString())).thenReturn(mockContainerClient);
    when(mockContainerClient.getBlobClient(anyString())).thenReturn(mockBlobClient);

    client = new AzureBlobStorageClient(mockServiceClient, 256 * 1024, 1024);
  }

  private BlobStorageException buildMockBlobStorageException(BlobErrorCode errorCode) {
    final BlobStorageException thrownException = mock(BlobStorageException.class);
    when(thrownException.getErrorCode()).thenReturn(errorCode);
    return thrownException;
  }

  private Path createTempFile() throws IOException {
    final Path tempFile = Files.createTempFile("test-file-", ".tmp");
    Files.write(tempFile, TieredStorageTestUtils.randomBytes(100));
    return tempFile;
  }

  // uploadBytesToBlob tests
  @Test
  void uploadBytesToBlobWhenContainerExists() {
    client.uploadBytesToBlob(TEST_CONTAINER_1, TEST_BLOB_1, TieredStorageTestUtils.randomBytes(4));

    verify(mockBlobClient).upload(any(BinaryData.class), eq(true));
  }

  @Test
  void uploadBytesToBlobWhenContainerDoesNotExist() {
    final BlobStorageException containerNotFoundException =
        buildMockBlobStorageException(BlobErrorCode.CONTAINER_NOT_FOUND);
    doThrow(containerNotFoundException)
        .doNothing()
        .when(mockBlobClient).upload(any(BinaryData.class), eq(true));

    // Should not throw exception
    client.uploadBytesToBlob(TEST_CONTAINER_1, TEST_BLOB_1, TieredStorageTestUtils.randomBytes(4));

    verify(mockContainerClient).create();
    verify(mockBlobClient, times(2)).upload(any(BinaryData.class), eq(true));
  }

  @Test
  void uploadBytesToBlobWhenContainerDoesNotExistThenThrowsAlreadyExists() {
    final BlobStorageException containerNotFoundException =
        buildMockBlobStorageException(BlobErrorCode.CONTAINER_NOT_FOUND);
    doThrow(containerNotFoundException)
        .doNothing()
        .when(mockBlobClient).upload(any(BinaryData.class), eq(true));

    final BlobStorageException containerAlreadyExistsException =
        buildMockBlobStorageException(BlobErrorCode.CONTAINER_ALREADY_EXISTS);
    doThrow(containerAlreadyExistsException)
        .doNothing()
        .when(mockContainerClient).create();

    // Should not throw exception
    client.uploadBytesToBlob(TEST_CONTAINER_1, TEST_BLOB_1, TieredStorageTestUtils.randomBytes(4));

    verify(mockContainerClient).create();
    verify(mockBlobClient, times(2)).upload(any(BinaryData.class), eq(true));
  }

  @Test
  void uploadBytesToBlobThrowsWhenContainerDoesNotExistThenFailsToCreate() {
    final BlobStorageException containerNotFoundException =
        buildMockBlobStorageException(BlobErrorCode.CONTAINER_NOT_FOUND);
    doThrow(containerNotFoundException)
        .when(mockBlobClient).upload(any(BinaryData.class), eq(true));

    final BlobStorageException internalErrorException =
        buildMockBlobStorageException(BlobErrorCode.INTERNAL_ERROR);
    doThrow(internalErrorException)
        .when(mockContainerClient).create();

    assertThrows(BlobStorageException.class,
        () -> client.uploadBytesToBlob(TEST_CONTAINER_1, TEST_BLOB_1, TieredStorageTestUtils.randomBytes(4)));

    verify(mockContainerClient).create();
    verify(mockBlobClient).upload(any(BinaryData.class), eq(true));
  }

  @Test
  void uploadBytesToBlobThrowsWhenOtherBlobStorageErrorThrown() {
    final BlobStorageException internalErrorException =
        buildMockBlobStorageException(BlobErrorCode.INTERNAL_ERROR);
    doThrow(internalErrorException)
        .when(mockBlobClient).upload(any(BinaryData.class), eq(true));

    assertThrows(BlobStorageException.class,
        () -> client.uploadBytesToBlob(TEST_CONTAINER_1, TEST_BLOB_1, TieredStorageTestUtils.randomBytes(4)));

    verify(mockBlobClient).upload(any(BinaryData.class), eq(true));
  }

  @Test
  void uploadBytesToBlobThrowsWhenUnknownThrown() {
    doThrow(new RuntimeException("Unexpected error"))
        .when(mockBlobClient).upload(any(BinaryData.class), eq(true));

    assertThrows(RuntimeException.class,
        () -> client.uploadBytesToBlob(TEST_CONTAINER_1, TEST_BLOB_1, TieredStorageTestUtils.randomBytes(4)));

    verify(mockBlobClient).upload(any(BinaryData.class), eq(true));
  }

  // uploadFileToBlob tests

  @Test
  void uploadFileToBlobWhenContainerExists() throws IOException {
    final Path tempFile = createTempFile();

    client.uploadFileToBlob(TEST_CONTAINER_1, TEST_BLOB_1, tempFile);

    verify(mockBlobClient)
        .uploadFromFile(eq(tempFile.toString()), any(), any(), any(), any(), any(), any());
  }

  @Test
  void uploadFileToBlobWhenContainerDoesNotExist() throws IOException {
    final Path tempFile = createTempFile();

    final BlobStorageException containerNotFoundException =
        buildMockBlobStorageException(BlobErrorCode.CONTAINER_NOT_FOUND);
    doThrow(containerNotFoundException)
        .doNothing()
        .when(mockBlobClient).uploadFromFile(eq(tempFile.toString()), any(), any(), any(), any(), any(), any());

    // Should not throw exception
    client.uploadFileToBlob(TEST_CONTAINER_1, TEST_BLOB_1, tempFile);

    verify(mockContainerClient).create();
    verify(mockBlobClient, times(2))
        .uploadFromFile(eq(tempFile.toString()), any(), any(), any(), any(), any(), any());

    Files.delete(tempFile);
  }

  @Test
  void uploadFileToBlobWhenContainerDoesNotExistThenThrowsAlreadyExists() throws IOException {
    final Path tempFile = createTempFile();

    final BlobStorageException containerNotFoundException =
        buildMockBlobStorageException(BlobErrorCode.CONTAINER_NOT_FOUND);
    doThrow(containerNotFoundException)
        .doNothing()
        .when(mockBlobClient).uploadFromFile(eq(tempFile.toString()), any(), any(), any(), any(), any(), any());

    final BlobStorageException containerAlreadyExistsException =
        buildMockBlobStorageException(BlobErrorCode.CONTAINER_ALREADY_EXISTS);
    doThrow(containerAlreadyExistsException)
        .doNothing()
        .when(mockContainerClient).create();

    // Should not throw exception
    client.uploadFileToBlob(TEST_CONTAINER_1, TEST_BLOB_1, tempFile);

    verify(mockContainerClient).create();
    verify(mockBlobClient, times(2))
        .uploadFromFile(eq(tempFile.toString()), any(), any(), any(), any(), any(), any());

    Files.delete(tempFile);
  }

  @Test
  void uploadFileToBlobThrowsWhenContainerDoesNotExistThenFailsToCreate() throws IOException {
    final Path tempFile = createTempFile();

    final BlobStorageException containerNotFoundException =
        buildMockBlobStorageException(BlobErrorCode.CONTAINER_NOT_FOUND);
    doThrow(containerNotFoundException)
        .when(mockBlobClient).uploadFromFile(eq(tempFile.toString()), any(), any(), any(), any(), any(), any());

    final BlobStorageException internalErrorException =
        buildMockBlobStorageException(BlobErrorCode.INTERNAL_ERROR);
    doThrow(internalErrorException)
        .when(mockContainerClient).create();

    assertThrows(BlobStorageException.class,
        () -> client.uploadFileToBlob(TEST_CONTAINER_1, TEST_BLOB_1, tempFile));

    verify(mockContainerClient).create();
    verify(mockBlobClient).uploadFromFile(eq(tempFile.toString()), any(), any(), any(), any(), any(), any());

    Files.delete(tempFile);
  }

  @Test
  void uploadFileToBlobThrowsWhenOtherBlobStorageErrorThrown() throws IOException {
    final Path tempFile = createTempFile();

    final BlobStorageException internalErrorException =
        buildMockBlobStorageException(BlobErrorCode.INTERNAL_ERROR);
    doThrow(internalErrorException)
        .when(mockBlobClient).uploadFromFile(eq(tempFile.toString()), any(), any(), any(), any(), any(), any());

    assertThrows(BlobStorageException.class,
        () -> client.uploadFileToBlob(TEST_CONTAINER_1, TEST_BLOB_1, tempFile));

    verify(mockBlobClient).uploadFromFile(eq(tempFile.toString()), any(), any(), any(), any(), any(), any());

    Files.delete(tempFile);
  }

  @Test
  void uploadFileToBlobThrowsWhenIOExceptionThrown() throws IOException {
    final Path tempFile = createTempFile();

    doThrow(new UncheckedIOException(new IOException("Unable to read bytes from disk")))
        .when(mockBlobClient).uploadFromFile(eq(tempFile.toString()), any(), any(), any(), any(), any(), any());

    assertThrows(UncheckedIOException.class,
        () -> client.uploadFileToBlob(TEST_CONTAINER_1, TEST_BLOB_1, tempFile));

    verify(mockBlobClient).uploadFromFile(eq(tempFile.toString()), any(), any(), any(), any(), any(), any());

    Files.delete(tempFile);
  }

  @Test
  void uploadFileThrowsUncheckedIOExceptionWhenFileSizeFails() {
    // Deliberately do not create a file at the given path, so that Files.size() call throws.

    assertThrows(UncheckedIOException.class, () ->
        client.uploadFileToBlob(TEST_CONTAINER_1, TEST_BLOB_1, Paths.get("non-existent-file")));

    verify(mockBlobClient)
        .uploadFromFile(eq("non-existent-file"), any(), any(), any(), any(), any(), any());
  }

  // fetchBlobAsBytes tests
  @Test
  void fetchBlobAsBytesSucceeds() {
    when(mockBlobClient.downloadContent())
        .thenReturn(BinaryData.fromBytes(TieredStorageTestUtils.randomBytes(100)));

    client.fetchBlobAsBytes(TEST_CONTAINER_1, TEST_BLOB_1);

    verify(mockBlobClient).downloadContent();
  }

  @Test
  void fetchBlobAsBytesThrowsNotFoundExceptionOnBlobNotFoundError() {
    final BlobStorageException blobNotFoundException = buildMockBlobStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    when(mockBlobClient.downloadContent())
        .thenThrow(blobNotFoundException);

    assertThrows(ResourceNotFoundException.class, () -> client.fetchBlobAsBytes(TEST_CONTAINER_1, TEST_BLOB_1));
  }

  @Test
  void fetchBlobAsBytesThrowsNotFoundExceptionOnContainerNotFoundError() {
    final BlobStorageException containerNotFoundException =
        buildMockBlobStorageException(BlobErrorCode.CONTAINER_NOT_FOUND);
    when(mockBlobClient.downloadContent())
        .thenThrow(containerNotFoundException);

    assertThrows(ResourceNotFoundException.class, () -> client.fetchBlobAsBytes(TEST_CONTAINER_1, TEST_BLOB_1));
  }

  @Test
  void fetchBlobAsBytesBubblesUpOtherBlobStorageException() {
    final BlobStorageException containerNotFoundException =
        buildMockBlobStorageException(BlobErrorCode.INTERNAL_ERROR);
    when(mockBlobClient.downloadContent())
        .thenThrow(containerNotFoundException);

    assertThrows(BlobStorageException.class, () -> client.fetchBlobAsBytes(TEST_CONTAINER_1, TEST_BLOB_1));
  }

  @Test
  void fetchBlobAsBytesBubblesUpOtherException() {
    when(mockBlobClient.downloadContent())
        .thenThrow(new UncheckedIOException(new IOException("Unable to write to disk")));

    assertThrows(UncheckedIOException.class, () -> client.fetchBlobAsBytes(TEST_CONTAINER_1, TEST_BLOB_1));
  }

  // downloadBlobToFile tests
  @Test
  void downloadBlobToFileSucceeds() {
    client.downloadBlobToFile(TEST_CONTAINER_1, TEST_BLOB_1, TEST_PATH_1);

    verify(mockBlobClient).downloadToFile(TEST_PATH_1.toString(), true);
  }

  @Test
  void downloadBlobToFileThrowsNotFoundExceptionOnBlobNotFoundError() {
    final BlobStorageException blobNotFoundException = buildMockBlobStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    when(mockBlobClient.downloadToFile(anyString(), eq(true)))
        .thenThrow(blobNotFoundException);

    assertThrows(ResourceNotFoundException.class, () ->
        client.downloadBlobToFile(TEST_CONTAINER_1, TEST_BLOB_1, TEST_PATH_1));
  }

  @Test
  void downloadBlobToFileThrowsNotFoundExceptionOnContainerNotFoundError() {
    final BlobStorageException containerNotFoundException =
        buildMockBlobStorageException(BlobErrorCode.CONTAINER_NOT_FOUND);
    when(mockBlobClient.downloadToFile(anyString(), eq(true)))
        .thenThrow(containerNotFoundException);

    assertThrows(ResourceNotFoundException.class, () ->
        client.downloadBlobToFile(TEST_CONTAINER_1, TEST_BLOB_1, TEST_PATH_1));
  }

  @Test
  void downloadBlobToFileBubblesUpOtherBlobStorageException() {
    final BlobStorageException containerNotFoundException =
        buildMockBlobStorageException(BlobErrorCode.INTERNAL_ERROR);
    when(mockBlobClient.downloadToFile(anyString(), eq(true)))
        .thenThrow(containerNotFoundException);

    assertThrows(BlobStorageException.class, () ->
        client.downloadBlobToFile(TEST_CONTAINER_1, TEST_BLOB_1, TEST_PATH_1));
  }

  @Test
  void downloadBlobToFileBubblesUpOtherException() {
    when(mockBlobClient.downloadToFile(anyString(), eq(true)))
        .thenThrow(new UncheckedIOException(new IOException("Unable to write to disk")));

    assertThrows(UncheckedIOException.class, () ->
        client.downloadBlobToFile(TEST_CONTAINER_1, TEST_BLOB_1, TEST_PATH_1));
  }

  @Test
  void deleteBlobSucceeds() {
    client.deleteBlob(TEST_CONTAINER_1, TEST_BLOB_1);

    verify(mockBlobClient).delete();
  }

  @Test
  void deleteBlobSucceedsIfBlobNotFound() {
    final BlobStorageException blobNotFoundException = buildMockBlobStorageException(BlobErrorCode.BLOB_NOT_FOUND);
    doThrow(blobNotFoundException)
        .when(mockBlobClient).delete();

    client.deleteBlob(TEST_CONTAINER_1, TEST_BLOB_1);

    verify(mockBlobClient).delete();
  }

  @Test
  void deleteBlobSucceedsIfContainerNotFound() {
    final BlobStorageException containerNotFoundException =
        buildMockBlobStorageException(BlobErrorCode.CONTAINER_NOT_FOUND);
    doThrow(containerNotFoundException)
        .when(mockBlobClient).delete();

    client.deleteBlob(TEST_CONTAINER_1, TEST_BLOB_1);

    verify(mockBlobClient).delete();
  }

  @Test
  void deleteBlobBubblesUpOtherBlobStorageException() {
    final BlobStorageException internalErrorException =
        buildMockBlobStorageException(BlobErrorCode.INTERNAL_ERROR);
    doThrow(internalErrorException)
        .when(mockBlobClient).delete();

    assertThrows(BlobStorageException.class, () -> client.deleteBlob(TEST_CONTAINER_1, TEST_BLOB_1));

    verify(mockBlobClient).delete();
  }

  @Test
  void deleteBlobBubblesUpOtherException() {
    doThrow(RuntimeException.class)
        .when(mockBlobClient).delete();

    assertThrows(RuntimeException.class, () -> client.deleteBlob(TEST_CONTAINER_1, TEST_BLOB_1));

    verify(mockBlobClient).delete();
  }
}
