/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.tiered.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static com.linkedin.kafka.tiered.storage.RemoteStorageManagerDefaults.*;


/**
 * A Collection of helper methods.
 */
public final class TieredStorageTestUtils {
  private static final Logger log = LoggerFactory.getLogger(TieredStorageTestUtils.class);

  /* A random number generator initialized with a seed to make tests repeatable */
  public static final Random RANDOM = new Random(987654321);

  private TieredStorageTestUtils() {
    // stateless class
  }

  /**
   * Generate an array of random bytes
   *
   * @param size The size of the array
   * @return random bytes
   */
  public static byte[] randomBytes(int size) {
    final byte[] bytes = new byte[size];
    RANDOM.nextBytes(bytes);
    return bytes;
  }

  /**
   * Create a temporary relative directory in the default temporary-file directory with the given prefix.
   *
   * @param prefix The prefix of the temporary directory, if null using "kafka-" as default prefix
   * @return A temporary directory
   */
  public static File tempDirectory(final String prefix) {
    return tempDirectory(null, prefix);
  }

  /**
   * Create a temporary relative directory in the specified parent directory with the given prefix.
   *
   * @param parent The parent folder path name, if null using the default temporary-file directory
   * @param prefix The prefix of the temporary directory, if null using "kafka-" as default prefix
   * @return A temporary directory
   */
  public static File tempDirectory(final Path parent, final String prefix) {
    final File file;
    try {
      file = parent == null
             ? Files.createTempDirectory(prefix).toFile()
             : Files.createTempDirectory(parent, prefix == null ? "kafka-" : prefix).toFile();
    } catch (final IOException ex) {
      throw new RuntimeException("Failed to create a temp dir", ex);
    }
    file.deleteOnExit();

    Exit.addShutdownHook("delete-temp-file-shutdown-hook", () -> {
      try {
        Utils.delete(file);
      } catch (IOException e) {
        log.error("Error deleting {}", file.getAbsolutePath(), e);
      }
    });

    return file;
  }

  /**
   * Create RemoteLogSegmentMetadata from topicname and partition
   * @param topic topicname
   * @param partition partition number
   * @return A valid RemoteLogSegmentMetadata
   */
  public static RemoteLogSegmentMetadata createRemoteLogSegmentMetadata(String topic, int partition) {
    TopicIdPartition topicPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(topic, partition));
    RemoteLogSegmentId id = new RemoteLogSegmentId(topicPartition, Uuid.randomUuid());
    return new RemoteLogSegmentMetadata(id, 100L, 200L, System.currentTimeMillis(), 0,
                                        System.currentTimeMillis(), 100, Collections.singletonMap(1, 100L));
  }

  public static int randomPartition() {
    return RANDOM.nextInt() % 1000;
  }

  /**
   * Get configs for Azurite test setup fot unit tests.
   * @return A map of configs
   */
  public static Map<String, ?> getAzuriteConfig() {
    final Map<String, Object> map = new HashMap<>();

    map.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_PROP, AZURITE_ACCOUNT_NAME);
    map.put(RSM_AZURE_BLOB_STORAGE_ACCOUNT_KEY_PROP, AZURITE_ACCOUNT_KEY);
    map.put(RSM_AZURE_BLOB_STORAGE_ENDPOINT_PROP, AZURITE_ENDPOINT);
    map.put(RSM_AZURE_BLOB_STORAGE_CONTAINER_NAME_PREFIX_PROP, "unit-test");

    return map;
  }
}
