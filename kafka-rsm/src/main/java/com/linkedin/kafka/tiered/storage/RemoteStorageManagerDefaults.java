/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.tiered.storage;

/**
 * This class encapsulates the common defaults and config keys shared across the main code and the tests.
 */
public final class RemoteStorageManagerDefaults {
  public static final String RSM_AZURE_BLOB_STORAGE_ACCOUNT = "azure.blob.storage.account";
  public static final String RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY = "azure.blob.storage.account.key";
  public static final String RSM_AZURE_BLOB_STORAGE_ENDPOINT = "azure.blob.storage.endpoint";

  // Well-known azurite account name, key, and the default host:port.
  // These coordinates are published by Azure at the following public URL.
  // https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=npm#well-known-storage-account-and-key
  public static final String AZURITE_ENDPOINT = "http://127.0.0.1:10000";
  public static final String AZURITE_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
  public static final String AZURITE_ACCOUNT_NAME = "devstoreaccount1";

  private RemoteStorageManagerDefaults() {
    // stateless class
  }
}
