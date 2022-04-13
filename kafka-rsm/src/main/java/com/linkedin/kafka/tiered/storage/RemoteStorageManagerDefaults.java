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

  private RemoteStorageManagerDefaults() {
    // stateless class
  }
}
