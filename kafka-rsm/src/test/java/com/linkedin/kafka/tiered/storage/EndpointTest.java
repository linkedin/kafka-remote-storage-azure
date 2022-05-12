/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.tiered.storage;

import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class EndpointTest {
  @Test
  void httpsWithPrivateEndpointButNoPortSucceeds() {
    final Endpoint endpoint = Endpoint.fromString("https://devtestaccount.privatelink.blob.core.windows.net");
    Assertions.assertEquals("https", endpoint.protocol());
    Assertions.assertEquals("devtestaccount.privatelink.blob.core.windows.net", endpoint.host());
    Assertions.assertFalse(endpoint.port().isPresent());
    Assertions.assertTrue(endpoint.isProduction());
    Assertions.assertTrue(endpoint.isSecure());
  }

  @Test
  void httpWithPortSucceeds() {
    final Endpoint endpoint = Endpoint.fromString("http://0.0.0.0:3400");
    Assertions.assertEquals("http", endpoint.protocol());
    Assertions.assertEquals("0.0.0.0", endpoint.host());
    Assertions.assertEquals(Optional.of("3400"), endpoint.port());
    Assertions.assertFalse(endpoint.isProduction());
    Assertions.assertFalse(endpoint.isSecure());
  }

  @Test
  void trailingColonWithoutPortThrows() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> Endpoint.fromString("https://0.0.0.0:"));
  }

  @Test
  void invalidProtocolThrows() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> Endpoint.fromString("ftp://0.0.0.0:3400"));
  }

  @Test
  void noHostnameThrows() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> Endpoint.fromString("https://:3400"));
  }
}
