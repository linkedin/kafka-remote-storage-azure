/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import java.util.Map;


/**
 * This class encapsulates the common defaults and config keys shared across the main code and the tests.
 */
public final class RemoteStorageManagerDefaults {
  public static final String RSM_AZURE_BLOB_STORAGE_ACCOUNT_PROP = "azure.blob.storage.account";
  public static final String RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY_PROP = "azure.blob.storage.account.key";
  public static final String RSM_AZURE_BLOB_STORAGE_ENDPOINT_PROP = "azure.blob.storage.endpoint";

  // Container name prefix is used to distinguish between multiple clusters with same name.
  // For example, different "metrics" clusters in the same region could have different prefix.
  public static final String RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_PROP = "azure.blob.storage.container.name.prefix";
  public static final String RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_DEFAULT = "";

  public static final int RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_MAX_LENGTH = 26;

  // Well-known azurite account name, key, and the default host:port.
  // These coordinates are published by Azure at the following public URL.
  // https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=npm#well-known-storage-account-and-key
  public static final String AZURITE_ENDPOINT = "http://127.0.0.1:10000";
  public static final String AZURITE_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
  public static final String AZURITE_ACCOUNT_NAME = "devstoreaccount1";

  private RemoteStorageManagerDefaults() {
    // stateless class
  }

  /**
   * Returns the container name prefix _if_ contained in the configs. Default otherwise.
   * @param configs The configs map.
   * @return Included container name prefix or the default.
   */
  public static String getOrDefaultContainerNamePrefix(Map<String, ?> configs) {
    String prefix = (String) configs.get(RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_PROP);
    if (prefix == null) {
      return RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_DEFAULT;
    }
    return prefix;
  }
}
