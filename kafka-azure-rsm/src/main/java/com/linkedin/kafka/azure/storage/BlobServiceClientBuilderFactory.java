/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import com.azure.storage.blob.BlobServiceClientBuilder;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.azure.storage.RemoteStorageManagerDefaults.*;


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
    String endpointConfig = (String) configs.get(RSM_AZURE_BLOB_STORAGE_ENDPOINT_PROP);
    String accountNameConfig = (String) configs.get(RSM_AZURE_BLOB_STORAGE_ACCOUNT_PROP);
    String accountKeyConfig = (String) configs.get(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY_PROP);

    if (endpointConfig == null || endpointConfig.trim().isEmpty()
        || accountNameConfig == null || accountNameConfig.trim().isEmpty()
        || accountKeyConfig == null || accountKeyConfig.trim().isEmpty()) {
      String msg = String.format("For Azure storage-key authentication, blob storage endpoint, account name, and account key must be provided. "
                                 + "found endpoint = '%s', found account name = '%s', found account key = '%s'",
                                 endpointConfig, accountNameConfig, accountKeyConfig);
      throw new IllegalArgumentException(msg);
    }

    Endpoint endpoint = Endpoint.fromString(endpointConfig);
    if (endpoint.isProduction() && !endpoint.isSecure()) {
      String msg = String.format("The endpoint '%s' appears to be a production endpoint without https.", endpointConfig);
      throw new IllegalArgumentException(msg);
    }

    String connectionString =
      String.format("DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;BlobEndpoint=%s/%s;", endpoint.protocol(), accountNameConfig,
                    accountKeyConfig, endpointConfig, accountNameConfig);
    log.info("Using connection string: {}", connectionString);
    return new BlobServiceClientBuilder().connectionString(connectionString);
  }
}
