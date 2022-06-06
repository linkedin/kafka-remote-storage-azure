/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import com.azure.storage.blob.BlobServiceClientBuilder;
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
   * @param config config object containing Azure Blob Storage endpoint, account name and key.
   * @return BlobServiceClientBuilder
   */
  public static BlobServiceClientBuilder getBlobServiceClientBuilder(AzureBlobRemoteStorageConfig config) {
    String endpointConfig = config.getEndpoint();
    String accountNameConfig = config.getAccountName();
    String accountKeyConfig = config.getAccountKey();

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
