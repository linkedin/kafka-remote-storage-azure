/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


/**
 * From Azure Blob Container name requirements
 * https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#container-names
 * 1. Container names must start or end with a letter or number, and can contain only letters, numbers, and the dash (-) character.
 * 2. Every dash (-) character must be immediately preceded and followed by a letter or number; consecutive dashes are not permitted.
 * 3. All letters in a container name must be lowercase.
 * 4. Container names must be from 3 through 63 characters long.
 */
public class ContainerNameEncoderTest {
  private static AzureBlobRemoteStorageManager azureBlobRemoteStorageManager = new AzureBlobRemoteStorageManager();
  static {
    azureBlobRemoteStorageManager.configure(TieredStorageTestUtils.getAzuriteConfig());
  }

  /**
   * Test against a collection of normal and wacky topic names.
   * @param topicName topic name
   */
  @ParameterizedTest
  @ValueSource(strings = {"f", "foo", "foo_v2", "foo-v3", "foo--v4", "FOO--v5", "FOO__v6", "f..v7", "foo.foo",
                          "foo._.foo", "foo--"})
  public void validContainerNameTest(String topicName) {
    RemoteLogSegmentMetadata remoteLogSegmentMetadata =
        TieredStorageTestUtils.createRemoteLogSegmentMetadata(topicName, TieredStorageTestUtils.randomPartition());
    String containerName = azureBlobRemoteStorageManager.getContainerName(remoteLogSegmentMetadata);

    Assertions.assertNotNull(containerName);
    // Begins with a lowercase letter or number
    Assertions.assertTrue(containerName.matches("[0-9a-z][0-9a-z\\-]*"));
    // Contains only letter, numbers, and dash character
    Assertions.assertTrue(containerName.matches("[0-9a-z\\-]+"));
    // Does not contain consecutive dashes.
    Assertions.assertFalse(containerName.contains("--"));
    // Contains no uppercase letters.
    Assertions.assertFalse(containerName.matches(".*[A-Z]+.*"));
    Assertions.assertTrue(containerName.length() >= 3);
    Assertions.assertTrue(containerName.length() <= 63);
  }

  @ParameterizedTest
  @ValueSource(strings = {"F", "aprefixexactly-27characters", "foo--foo", "-foo", "foo--", "foo.", "foo_", " f "})
  public void validContainerNamesTest(String prefix) {
    Assertions.assertThrows(IllegalArgumentException.class, () -> ContainerNameEncoder.validate(prefix));
  }
}
