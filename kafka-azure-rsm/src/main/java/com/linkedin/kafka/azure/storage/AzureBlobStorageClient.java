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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** This class provides a facade to the Azure Blob Storage API, allowing operations like upload, download and deletion
 * of blobs. This class handles client-side errors to make the operations idempotent where possible. For example,
 * if a blob deletion fails with "BLOB_NOT_FOUND" error, this client will not throw an exception, but move forward.
 * This commonly happens when distributed systems have multiple clients trying to perform the same operation, or
 * retrying an operation which may have partially succeeded.
 *
 * This client only throws unchecked exceptions. This makes it easier to use in other components that do not want to
 * explicitly handle failures, but pass them upwards, but are unable to change their signatures to throw
 * a checked exception. Thus, when there is an underlying {@link IOException}, this client will throw
 * an {@link UncheckedIOException}.
 *
 * If a blob or container does not exist at an expected location, this client translates that exception into a
 * {@link ResourceNotFoundException}, for ease of use of the client, who may choose to treat this condition specially.
  */
public class AzureBlobStorageClient {
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobRemoteStorageManager.class);
  private static final Set<BlobErrorCode> NOT_FOUND_ERRORS =
      Set.of(BlobErrorCode.BLOB_NOT_FOUND, BlobErrorCode.CONTAINER_NOT_FOUND);

  private final BlobServiceClient blobServiceClient;
  private final long maxSingleUploadSize;
  private final long blockSize;

  public AzureBlobStorageClient(BlobServiceClient blobServiceClient, long maxSingleUploadSize, long blockSize) {
    this.blobServiceClient = blobServiceClient;
    this.maxSingleUploadSize = maxSingleUploadSize;
    this.blockSize = blockSize;
  }

  private void createBlobContainer(String containerName) {
    BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
    try {
      blobContainerClient.create();
    } catch (BlobStorageException error) {
      if (!error.getErrorCode().equals(BlobErrorCode.CONTAINER_ALREADY_EXISTS)) {
        throw error;
      }
    }
  }

  /**
   * Perform an action and produce a value. If the action fails because of a ContainerNotFound exception,
   * this method will create the container and then re-try the action.
   * @param containerName Name of the container to create if not found.
   * @param supplier The action that needs to be performed, this should return a value of type T. This can
   *                 be a function. This method can throw a BlobStorageException with ContainerNotFound error.
   * @param <T> The type of the value returned by the supplier.
   * @return The value returned by the supplier.
   */
  private <T> T withLazyContainerCreation(String containerName, Supplier<T> supplier) {
    try {
      return supplier.get();
    } catch (BlobStorageException error) {
      if (error.getErrorCode().equals(BlobErrorCode.CONTAINER_NOT_FOUND)) {
        LOG.info("Container {} not found. Creating container.",
            containerName);
        createBlobContainer(containerName);
        return supplier.get();
      } else {
        throw error;
      }
    }
  }

  /**
   * Upload a byte array to a block blob. This method will overwrite an existing blob at the same location.
   * If a container does not exist with the given name, a new one will be created before the upload.
   *
   * @param containerName Name of the Azure Blob Storage container where the blob is to be uploaded.
   * @param blobName Name of the blob where the byte array needs to be uploaded.
   * @param data The array of bytes that needs to be uploaded.
   * @return Length of the array of bytes that were uploaded.
   *
   * @throws BlobStorageException if an error is received from Azure Blob Storage while uploading the blob.
   * @throws UncheckedIOException if there is an IO exception when uploading the blob.
   */
  public long uploadBytesToBlob(String containerName, String blobName, byte[] data) {
    return withLazyContainerCreation(containerName, () -> {
      BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
      final BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
      blobClient.upload(BinaryData.fromBytes(data), true);
      return data.length;
    });
  }

  /**
   * Upload a file on disk to a block blob. This method will overwrite an existing blob at the same location.
   * If a container does not exist with the given name, a new one will be created before the upload.
   *
   * @param containerName Name of the Azure Blob Storage container where the blob is to be uploaded.
   * @param blobName Name of the blob where the byte array needs to be uploaded.
   * @param inputFilePath The path to the file that needs to be uploaded.
   * @return Size of the file that was uploaded.
   *
   * @throws BlobStorageException if an error is received from Azure Blob Storage while uploading the blob.
   * @throws UncheckedIOException if there is an IO exception when uploading the blob.
   */
  public long uploadFileToBlob(String containerName, String blobName, Path inputFilePath) {
    return withLazyContainerCreation(containerName, () -> {
      BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
      final BlobClient blobClient = blobContainerClient.getBlobClient(blobName);

      ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions();
      parallelTransferOptions.setMaxSingleUploadSizeLong(maxSingleUploadSize);
      parallelTransferOptions.setBlockSizeLong(blockSize);

      blobClient.uploadFromFile(inputFilePath.toString(), parallelTransferOptions, null, null, null, null, null);
      try {
        return Files.size(inputFilePath);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
  }

  private RuntimeException maybeTransformToResourceNotFoundException(
      String containerName, String blobName, BlobStorageException error) {
    if (error.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
      return new ResourceNotFoundException("Blob {} not found in container {}", blobName, containerName, error);
    } else if (error.getErrorCode().equals(BlobErrorCode.CONTAINER_NOT_FOUND)) {
      return new ResourceNotFoundException("Container {} not found", containerName, error);
    }
    return error;
  }

  /**
   * Fetch a blob as a byte array. This method expects that the file is lower than 2 GB
   * ({@link Integer#MAX_VALUE}) in size.
   *
   * @param containerName Name of the Azure Blob Storage container from which the blob is to be fetched.
   * @param blobName Name of the blob to be fetched.
   * @return An array containing the bytes of the blob that was fetched.
   *
   * @throws BlobStorageException if an error is received from Azure Blob Storage when fetching the blob.
   * @throws UncheckedIOException if there is an IO exception when fetching the blob.
   * @throws ResourceNotFoundException if there is no blob or container in Azure with the name provided.
   */
  public byte[] fetchBlobAsBytes(String containerName, String blobName) {
    BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
    final BlobClient blobClient = blobContainerClient.getBlobClient(blobName);

    try {
      // Kafka uses an int to denote log segment size, so we are guaranteed to not have files > 2 GB.
      return blobClient.downloadContent().toBytes();
    } catch (BlobStorageException error) {
      throw maybeTransformToResourceNotFoundException(containerName, blobName, error);
    }
  }

  /**
   * Download a blob to a file on disk.
   *
   * @param containerName Name of the Azure Blob Storage container from which the blob is to be downloaded.
   * @param blobName Name of the blob to be downloaded.
   * @param outputFilePath The path to the file to which to download the Azure blob.
   *
   * @throws BlobStorageException if an error is received from Azure Blob Storage when downloading the blob.
   * @throws UncheckedIOException if there is an IO exception when downloading the blob to disk.
   * @throws ResourceNotFoundException if there is no blob or container in Azure with the name provided.
   */
  public void downloadBlobToFile(String containerName, String blobName, Path outputFilePath) {
    BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
    final BlobClient blobClient = blobContainerClient.getBlobClient(blobName);

    try {
      blobClient.downloadToFile(outputFilePath.toString(), true);
    } catch (BlobStorageException error) {
      throw maybeTransformToResourceNotFoundException(containerName, blobName, error);
    }
  }

  /**
   * Delete a blob from Azure Blob Storage. If the blob / container is not found at the location, this method
   * will consider that to be a no-op, and not throw an exception.
   *
   * @param containerName Name of the Azure Blob Storage container in which the blob is located.
   * @param blobName Name of the blob to be deleted.
   *
   * @throws BlobStorageException if an error is received from Azure Blob Storage when deleting the blob.
   */
  public void deleteBlob(String containerName, String blobName) {
    BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
    final BlobClient blobClient = blobContainerClient.getBlobClient(blobName);

    try {
      blobClient.delete();
    } catch (BlobStorageException error) {
      if (!NOT_FOUND_ERRORS.contains(error.getErrorCode())) {
        throw error;
      }
    }
  }

  /**
   * Check if a blob exists in Azure Blob Storage at a given location.
   * @param containerName Name of the Azure Blob Storage container where the blob should exist.
   * @param blobName Name of the blob.
   * @return true if a blob exists at the given container and blob name, false otherwise.
   */
  public boolean doesBlobExist(String containerName, String blobName) {
    BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
    final BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
    return blobClient.exists();
  }
}
