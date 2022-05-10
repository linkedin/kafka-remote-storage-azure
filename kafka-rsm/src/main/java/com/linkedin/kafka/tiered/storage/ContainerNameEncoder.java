/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.tiered.storage;

import java.util.UUID;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;


public class ContainerNameEncoder {
  private final String containerNamePrefix;

  /**
   * Create a container name encoder using the specified prefix.
   * @param containerNamePrefix The prefix. Possibly null.
   */
  public ContainerNameEncoder(String containerNamePrefix) {
    this.containerNamePrefix = validate(containerNamePrefix);
  }

  /**
   * Returns the container name for the given RemoteLogSegmentMetadata
   * @param remoteLogSegmentMetadata The remote log segment metadata
   * @return A unique string name for the container for the remote log segment.
   */
  public String encode(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
    // See Azure Blob Container name requirements
    // https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#container-names
    final Uuid kafkaTopicUuid = remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicId();
    final UUID javaTopicUuid = new UUID(kafkaTopicUuid.getMostSignificantBits(), kafkaTopicUuid.getLeastSignificantBits());
    String lowerCaseTopicId = javaTopicUuid.toString().toLowerCase();
    String containerName = containerNamePrefix.isEmpty() ? lowerCaseTopicId : String.format("%s-%s", containerNamePrefix, lowerCaseTopicId);
    return containerName;
  }

  static String validate(String prefix) {
    if (prefix == null) {
      return RemoteStorageManagerDefaults.RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_DEFAULT;
    } else if (prefix.trim().isEmpty()) {
      return prefix.trim();
    } else {
      boolean valid = true;
      // Prefix starts with a lowercase letter or a number and not a hyphen.
      // contains only lowercase letters, numbers, and hyphens
      valid = valid && prefix.matches("[0-9a-z][0-9a-z\\-]*");
      // does not end with a hyphen
      valid = valid && !prefix.matches("[-]+$");
      // Prefix does not contain consecutive dashes.
      valid = valid && !prefix.contains("--");
      // Prefix is not too long
      valid = valid && prefix.length() <= RemoteStorageManagerDefaults.RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_MAX_LENGTH;

      if (valid) {
        return prefix;
      } else {
        throw new IllegalArgumentException(String.format("Container name '%s' is not valid.", prefix));
      }
    }
  }
}
