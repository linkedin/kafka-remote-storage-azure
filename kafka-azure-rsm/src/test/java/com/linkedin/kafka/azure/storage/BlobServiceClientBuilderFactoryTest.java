/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import com.azure.storage.blob.BlobServiceClientBuilder;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import static com.linkedin.kafka.azure.storage.AzureBlobRemoteStorageConfig.*;
import static com.linkedin.kafka.azure.storage.RemoteStorageManagerDefaults.*;


public class BlobServiceClientBuilderFactoryTest {
  @Test
  public void testEndpintUrlValidation() {
    Map<String, String> blobStorageConfigs = new HashMap<>(3);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ENDPOINT_PROP, AZURITE_ENDPOINT);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_PROP, AZURITE_ACCOUNT_NAME);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY_PROP, AZURITE_ACCOUNT_KEY);

    BlobServiceClientBuilder builder = BlobServiceClientBuilderFactory.getBlobServiceClientBuilder(
        new AzureBlobRemoteStorageConfig(blobStorageConfigs));
    Assertions.assertNotNull(builder);

    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ENDPOINT_PROP, "http://illegal_host:port");
    Assertions.assertThrows(IllegalArgumentException.class, () -> BlobServiceClientBuilderFactory.getBlobServiceClientBuilder(
        new AzureBlobRemoteStorageConfig(blobStorageConfigs)));
  }
}
