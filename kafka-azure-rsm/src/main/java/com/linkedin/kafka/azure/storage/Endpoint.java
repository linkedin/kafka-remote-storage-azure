/*
 * Copyright 2022 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.azure.storage;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Endpoint {
  /* Some valid strings that match this regex
    azurite: http://127.0.0.1:10000
    private endpoint: https://accountname.blob.core.windows.net
    The regex goes like this -
    1. http or https at the start of string ^(https?) (first capturing group)
    2. ://
    3. group of characters other than a : ([^:]+) (second capturing group for hostname)
    4. an optional non-capturing group that starts with : followed by one or more digits
       and is at the end of the string (?::(\d+))?$
       This group nests another capturing group for the group of digits.
   */
  public static final String BLOB_ENDPOINT_REGEX_PATTERN = "^(https?)://([^:]+)(?::(\\d+))?$";

  private final String protocol;
  private final String host;
  private final Optional<String> port;

  Endpoint(String protocol, String host, String port) {
    this.protocol = protocol;
    this.host = host;
    this.port = Optional.ofNullable(port);
  }

  public String protocol() {
    return protocol;
  }

  public String host() {
    return host;
  }

  public Optional<String> port() {
    return port;
  }

  static Endpoint fromString(String endpointConfig) {
    Pattern pattern = Pattern.compile(BLOB_ENDPOINT_REGEX_PATTERN);
    Matcher matcher = pattern.matcher(endpointConfig);
    if (matcher.matches()) {
      if (matcher.group(3) == null) {
        return new Endpoint(matcher.group(1).trim(), matcher.group(2).trim(), null);
      } else {
        return new Endpoint(matcher.group(1).trim(), matcher.group(2).trim(), matcher.group(3).trim());
      }
    }
    throw new IllegalArgumentException(String.format("Invalid blob endpoint '%s'", endpointConfig));
  }

  public boolean isProduction() {
    // The DNS naming standard for the private endpoint for the blob service is having the
    // blob.core.windows.net suffix.
    // See https://docs.microsoft.com/en-us/azure/storage/common/storage-private-endpoints#dns-changes-for-private-endpoints
    // Nephos also adheres to this standard.
    return host.endsWith("blob.core.windows.net");
  }

  public boolean isSecure() {
    return "https".equals(protocol);
  }
}
