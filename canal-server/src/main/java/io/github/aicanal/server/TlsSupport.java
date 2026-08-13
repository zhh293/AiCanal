package io.github.aicanal.server;

import io.netty.handler.ssl.*;
import java.io.IOException;

final class TlsSupport {
  private TlsSupport() {}

  static SslContext build(ServerConfig config) {
    if (!config.isTlsEnabled()) return null;
    try {
      SslContextBuilder builder =
          SslContextBuilder.forServer(
                  config.getTlsCertificate().toFile(), config.getTlsPrivateKey().toFile())
              .protocols("TLSv1.3", "TLSv1.2");
      if (config.getTlsTrustCertificate() != null)
        builder.trustManager(config.getTlsTrustCertificate().toFile());
      builder.clientAuth(config.isTlsClientAuthRequired() ? ClientAuth.REQUIRE : ClientAuth.NONE);
      return builder.build();
    } catch (IOException e) {
      throw new IllegalArgumentException("cannot load TCP TLS material", e);
    }
  }
}
