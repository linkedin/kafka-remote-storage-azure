/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import org.slf4j.helpers.MessageFormatter;


/**
 * This exception is thrown internally by the {@link AzureBlobStorageClient} when a blob or container
 * that is expected to exist at a certain location does not exist. This exception is not exposed to the
 * callers of the RSM plugin, and is translated internally into a checked
 * exception {@link AzureResourceNotFoundException}.
 *
 * This is done so that the {@link AzureBlobStorageClient} can only throw unchecked exceptions, but still indicate
 * to clients when a resource is not found.
 */
public class ResourceNotFoundException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  /**
   * This method provides an easy way to construct error messages for the exception, similar
   * to the Slf4j API. A client can call it like so -
   * new ResourceNotFoundException("Error for blob {}", blob, exception)
   *
   * @param message Error message with placeholders for arguments denoted by a '{}'.
   * @param arguments List of arguments to fill the placeholders in the message string.
   *                  An extra argument can be supplied at the end if it is a Throwable,
   *                  and is recorded as the cause of this exception.
   */
  public ResourceNotFoundException(final String message, Object... arguments) {
    super(MessageFormatter.arrayFormat(message, arguments).getMessage(),
        MessageFormatter.arrayFormat(message, arguments).getThrowable());
  }
}
