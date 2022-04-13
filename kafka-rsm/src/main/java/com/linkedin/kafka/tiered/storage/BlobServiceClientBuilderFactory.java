/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.tiered.storage;

import com.azure.storage.blob.BlobServiceClientBuilder;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.tiered.storage.RemoteStorageManagerDefaults.*;


/**
 * This is a helper class to create {@link BlobServiceClientBuilder} from the configs.
 */
public final class BlobServiceClientBuilderFactory {
  private static final Logger log = LoggerFactory.getLogger(BlobServiceClientBuilderFactory.class);

  private BlobServiceClientBuilderFactory() {
    // stateless class
  }

  /**
   * Create {@link BlobServiceClientBuilder}
   * @param configs configs containing Azure Blob Storage endpoint, account name and key.
   * @return BlobServiceClientBuilder
   */
  public static BlobServiceClientBuilder getBlobServiceClientBuilder(Map<String, ?> configs) {
    String endpoint = (String) configs.get(RSM_AZURE_BLOB_STORAGE_ENDPOINT);
    String accountName = (String) configs.get(RSM_AZURE_BLOB_STORAGE_ACCOUNT);
    String accountKey = (String) configs.get(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY);

    if (endpoint == null || endpoint.trim().isEmpty()
        || accountName == null || accountName.trim().isEmpty()
        || accountKey == null || accountKey.trim().isEmpty()) {
      String msg = String.format("For Azure storage-key authentication, blob storage endpoint, account name, and account key must be provided. "
                                 + "found endpoint = '%s', found account name = '%s', found account key = '%s'", endpoint, accountName, accountKey);
      throw new IllegalArgumentException(msg);
    }

    String protocol;
    protocol = endpoint.matches("^https://.+:\\d+$") ? "https" : null;
    if (protocol == null) {
      protocol = endpoint.matches("^http://.+:\\d+$") ? "http" : null;
    }
    if (protocol == null) {
      throw new IllegalArgumentException(String.format("Invalid endpoint '%s'", endpoint));
    }

    String connectionString =
      String.format("DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;", protocol, accountName,
                    accountKey, endpoint, accountName);
    log.info("Using connection string: {}", connectionString);
    return new BlobServiceClientBuilder().connectionString(connectionString);
  }
}
