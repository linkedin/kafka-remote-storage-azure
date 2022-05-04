/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.tiered.storage;

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;


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
}
