/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.tiered.storage;

import com.azure.storage.blob.BlobServiceClientBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import static com.linkedin.kafka.tiered.storage.RemoteStorageManagerDefaults.RSM_AZURE_BLOB_STORAGE_ACCOUNT;
import static com.linkedin.kafka.tiered.storage.RemoteStorageManagerDefaults.RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY;
import static com.linkedin.kafka.tiered.storage.RemoteStorageManagerDefaults.RSM_AZURE_BLOB_STORAGE_ENDPOINT;
import static com.linkedin.kafka.tiered.storage.RemoteStorageManagerDefaults.AZURITE_ENDPOINT;
import static com.linkedin.kafka.tiered.storage.RemoteStorageManagerDefaults.AZURITE_ACCOUNT_KEY;
import static com.linkedin.kafka.tiered.storage.RemoteStorageManagerDefaults.AZURITE_ACCOUNT_NAME;


public class BlobServiceClientBuilderFactoryTest {
  private void assertException(Callable<BlobServiceClientBuilder> codeFragment, Class expectedExceptionClass) {
    try {
      codeFragment.call();
      Assertions.fail();
    } catch (Exception ex) {
      Assertions.assertTrue(expectedExceptionClass.isAssignableFrom(ex.getClass()));
    }
  }

  @Test
  public void testRequiredKeysValidation() {
    Map<String, String> blobStorageConfigs = new HashMap<>(3);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ENDPOINT, AZURITE_ENDPOINT);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT, AZURITE_ACCOUNT_NAME);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY, AZURITE_ACCOUNT_KEY);
    BlobServiceClientBuilder builder = BlobServiceClientBuilderFactory.getBlobServiceClientBuilder(blobStorageConfigs);
    Assertions.assertNotNull(builder);
  }

  @Test
  public void testMissingKeysValidation() {
    Map<String, String> blobStorageConfigs1 = new HashMap<>();
    blobStorageConfigs1.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT, AZURITE_ACCOUNT_NAME);
    blobStorageConfigs1.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY, AZURITE_ACCOUNT_KEY);
    assertException(() -> BlobServiceClientBuilderFactory.getBlobServiceClientBuilder(blobStorageConfigs1), IllegalArgumentException.class);

    Map<String, String> blobStorageConfigs2 = new HashMap<>();
    blobStorageConfigs2.put(RSM_AZURE_BLOB_STORAGE_ENDPOINT, AZURITE_ENDPOINT);
    blobStorageConfigs2.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY, AZURITE_ACCOUNT_KEY);
    assertException(() -> BlobServiceClientBuilderFactory.getBlobServiceClientBuilder(blobStorageConfigs2), IllegalArgumentException.class);

    Map<String, String> blobStorageConfigs3 = new HashMap<>();
    blobStorageConfigs3.put(RSM_AZURE_BLOB_STORAGE_ENDPOINT, AZURITE_ENDPOINT);
    blobStorageConfigs3.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT, AZURITE_ACCOUNT_NAME);
    assertException(() -> BlobServiceClientBuilderFactory.getBlobServiceClientBuilder(blobStorageConfigs3), IllegalArgumentException.class);
  }

  @Test
  public void testEndpintUrlValidation() {
    Map<String, String> blobStorageConfigs = new HashMap<>(3);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ENDPOINT, AZURITE_ENDPOINT);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT, AZURITE_ACCOUNT_NAME);
    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY, AZURITE_ACCOUNT_KEY);
    BlobServiceClientBuilder builder = BlobServiceClientBuilderFactory.getBlobServiceClientBuilder(blobStorageConfigs);
    Assertions.assertNotNull(builder);

    blobStorageConfigs.put(RSM_AZURE_BLOB_STORAGE_ENDPOINT, "http://illegal_host:port");
    assertException(() -> BlobServiceClientBuilderFactory.getBlobServiceClientBuilder(blobStorageConfigs), IllegalArgumentException.class);
  }
}
