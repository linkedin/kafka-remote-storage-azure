/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import java.util.Map;


public class AzureBlobRemoteStorageConfig {
  public static final String RSM_AZURE_BLOB_STORAGE_ACCOUNT_PROP = "azure.blob.storage.account";
  public static final String RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY_PROP = "azure.blob.storage.account.key";
  public static final String RSM_AZURE_BLOB_STORAGE_ENDPOINT_PROP = "azure.blob.storage.endpoint";
  public static final String RSM_AZURE_BLOB_STORAGE_MAX_SINGLE_UPLOAD_SIZE_PROP =
      "azure.blob.storage.max.single.upload.size";
  public static final String RSM_AZURE_BLOB_STORAGE_BLOCK_SIZE_PROP = "azure.blob.storage.block.size";
  // Container name prefix is used to distinguish between multiple clusters with same name.
  // For example, different "metrics" clusters in the same region could have different prefix.
  public static final String RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_PROP = "azure.blob.storage.container.name.prefix";
  public static final String RSM_AZURE_BLOB_STORAGE_CACHE_BYTES_PROP = "azure.blob.storage.cache.bytes";

  private final String endpoint;
  private final String accountName;
  private final String accountKey;
  private final long maxSingleUploadSize;
  private final long blockSize;
  private final String containerNamePrefix;
  private final long cacheBytes;

  public AzureBlobRemoteStorageConfig(Map<String, ?> configs) {
    this.endpoint = (String) configs.get(RSM_AZURE_BLOB_STORAGE_ENDPOINT_PROP);
    this.accountName = (String) configs.get(RSM_AZURE_BLOB_STORAGE_ACCOUNT_PROP);
    this.accountKey = (String) configs.get(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY_PROP);

    if (endpoint == null || endpoint.trim().isEmpty() || accountName == null || accountName.trim().isEmpty()
        || accountKey == null || accountKey.trim().isEmpty()) {
      String msg = String.format(
          "For Azure storage-key authentication, blob storage endpoint, account name, and account key must be provided. "
              + "found endpoint = '%s', found account name = '%s', found account key = '%s'", endpoint, accountName,
          accountKey);
      throw new IllegalArgumentException(msg);
    }

    this.maxSingleUploadSize = getLongValueOrDefaultForProp(configs, RSM_AZURE_BLOB_STORAGE_MAX_SINGLE_UPLOAD_SIZE_PROP,
        RemoteStorageManagerDefaults.RSM_AZURE_BLOB_STORAGE_MAX_SINGLE_UPLOAD_SIZE_DEFAULT);
    this.blockSize = getLongValueOrDefaultForProp(configs, RSM_AZURE_BLOB_STORAGE_BLOCK_SIZE_PROP,
        RemoteStorageManagerDefaults.RSM_AZURE_BLOB_STORAGE_BLOCK_SIZE_DEFAULT);

    String containerNamePrefixValue = (String) configs.get(RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_PROP);
    this.containerNamePrefix = containerNamePrefixValue != null ? containerNamePrefixValue
        : RemoteStorageManagerDefaults.RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_DEFAULT;
    this.cacheBytes = getLongValueOrDefaultForProp(configs, RSM_AZURE_BLOB_STORAGE_CACHE_BYTES_PROP,
        RemoteStorageManagerDefaults.RSM_AZURE_BLOB_STORAGE_CACHE_BYTES_DEFAULT);
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getAccountName() {
    return accountName;
  }

  public String getAccountKey() {
    return accountKey;
  }

  public Long getMaxSingleUploadSize() {
    return maxSingleUploadSize;
  }

  public Long getBlockSize() {
    return blockSize;
  }

  public String getContainerNamePrefix() {
    return containerNamePrefix;
  }

  public long getCacheBytes() {
    return cacheBytes;
  }

  private long getLongValueOrDefaultForProp(Map<String, ?> configs, String propKey, long defaultValue) {
    final String propValueString = (String) configs.get(propKey);
    return propValueString == null ? defaultValue : Long.parseLong(propValueString);
  }
}
