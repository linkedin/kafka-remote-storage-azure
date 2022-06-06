/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.linkedin.kafka.azure.storage.AzureBlobRemoteStorageConfig.*;
import static com.linkedin.kafka.azure.storage.RemoteStorageManagerDefaults.*;


public class AzureBlobRemoteStorageConfigTest {
  @Test
  public void testAllKeysValidation() {
    Map<String, String> blobStorageConfigs = new HashMap<>();
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ENDPOINT_PROP, AZURITE_ENDPOINT);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_PROP, AZURITE_ACCOUNT_NAME);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY_PROP, AZURITE_ACCOUNT_KEY);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_MAX_SINGLE_UPLOAD_SIZE_PROP, "8388608");
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_BLOCK_SIZE_PROP, "2097152");
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_PROP, "prefix-");

    final AzureBlobRemoteStorageConfig config = new AzureBlobRemoteStorageConfig(blobStorageConfigs);
    Assertions.assertEquals(AZURITE_ENDPOINT, config.getEndpoint());
    Assertions.assertEquals(AZURITE_ACCOUNT_NAME, config.getAccountName());
    Assertions.assertEquals(AZURITE_ACCOUNT_KEY, config.getAccountKey());
    Assertions.assertEquals("prefix-", config.getContainerNamePrefix());
    Assertions.assertEquals(8_388_608, config.getMaxSingleUploadSize());
    Assertions.assertEquals(2_097_152, config.getBlockSize());
  }

  @Test
  public void testRequiredKeysValidation() {
    Map<String, String> blobStorageConfigs = new HashMap<>();
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ENDPOINT_PROP, AZURITE_ENDPOINT);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_PROP, AZURITE_ACCOUNT_NAME);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY_PROP, AZURITE_ACCOUNT_KEY);
    final AzureBlobRemoteStorageConfig config = new AzureBlobRemoteStorageConfig(blobStorageConfigs);
    Assertions.assertEquals(AZURITE_ENDPOINT, config.getEndpoint());
    Assertions.assertEquals(AZURITE_ACCOUNT_NAME, config.getAccountName());
    Assertions.assertEquals(AZURITE_ACCOUNT_KEY, config.getAccountKey());
    Assertions.assertEquals(RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_DEFAULT, config.getContainerNamePrefix());
    Assertions.assertEquals(RSM_AZURE_BLOB_STORAGE_MAX_SINGLE_UPLOAD_SIZE_DEFAULT, config.getMaxSingleUploadSize());
    Assertions.assertEquals(RSM_AZURE_BLOB_STORAGE_BLOCK_SIZE_DEFAULT, config.getBlockSize());
  }

  @Test
  void testMissingKeysValidation() {
    Map<String, String> blobStorageConfigs1 = new HashMap<>();
    blobStorageConfigs1.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_PROP, AZURITE_ACCOUNT_NAME);
    blobStorageConfigs1.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY_PROP, AZURITE_ACCOUNT_KEY);
    Assertions.assertThrows(IllegalArgumentException.class, () -> new AzureBlobRemoteStorageConfig(blobStorageConfigs1));

    Map<String, String> blobStorageConfigs2 = new HashMap<>();
    blobStorageConfigs2.put(RSM_AZURE_BLOB_STORAGE_ENDPOINT_PROP, AZURITE_ENDPOINT);
    blobStorageConfigs2.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY_PROP, AZURITE_ACCOUNT_KEY);
    Assertions.assertThrows(IllegalArgumentException.class, () -> new AzureBlobRemoteStorageConfig(blobStorageConfigs2));

    Map<String, String> blobStorageConfigs3 = new HashMap<>();
    blobStorageConfigs3.put(RSM_AZURE_BLOB_STORAGE_ENDPOINT_PROP, AZURITE_ENDPOINT);
    blobStorageConfigs3.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_PROP, AZURITE_ACCOUNT_NAME);
    Assertions.assertThrows(IllegalArgumentException.class, () -> new AzureBlobRemoteStorageConfig(blobStorageConfigs3));
  }
}
