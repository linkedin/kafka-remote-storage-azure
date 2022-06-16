/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.slf4j.helpers.MessageFormatter;


/**
 * This is thrown by the Azure RSM plugin when a resource (blob or container) is not found at the desired location
 * in Azure Blob Storage. This extends
 * the {@link RemoteResourceNotFoundException}, and is handled by the caller of the RSM methods.
 */
public class AzureResourceNotFoundException extends RemoteResourceNotFoundException {
  private static final long serialVersionUID = 1L;

  public AzureResourceNotFoundException(final String message) {
    super(message);
  }

  /**
   * This method provides an easy way to construct error messages for the exception, similar
   * to the Slf4j API. A client can call it like so -
   * new AzureBlobResourceNotFoundException("Error for blob {}", blob, exception)
   *
   * @param message Error message with placeholders for arguments denoted by a '{}'.
   * @param arguments List of arguments to fill the placeholders in the message string.
   *                  An extra argument can be supplied at the end if it is a Throwable,
   *                  and is recorded as the cause of this exception.
   */
  public AzureResourceNotFoundException(final String message, Object... arguments) {
    super(MessageFormatter.arrayFormat(message, arguments).getMessage(),
        MessageFormatter.arrayFormat(message, arguments).getThrowable());
  }

  public AzureResourceNotFoundException(final Throwable cause) {
    super(cause);
  }
}
