/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage.cache;

import java.util.Objects;

public class AzureLocation {
  private final String containerName;
  private final String blobName;

  public AzureLocation(final String containerName, final String blobName) {
    if (containerName == null || blobName == null) {
      throw new IllegalArgumentException("containerName and blobName cannot be null");
    }
    this.containerName = containerName;
    this.blobName = blobName;
  }

  public String getContainerName() {
    return containerName;
  }

  public String getBlobName() {
    return blobName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AzureLocation that = (AzureLocation) o;
    return containerName.equals(that.containerName) && blobName.equals(that.blobName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(containerName, blobName);
  }

  @Override
  public String toString() {
    return "AzureLocation{" + "containerName='" + containerName + '\'' + ", blobName='" + blobName + '\'' + '}';
  }
}
