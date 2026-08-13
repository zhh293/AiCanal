package io.github.aicanal.cluster.raft;

import java.net.*;
import java.util.Objects;

final class RaftPeer {
  final String nodeId;
  final InetSocketAddress address;

  private RaftPeer(String nodeId, InetSocketAddress address) {
    this.nodeId = nodeId;
    this.address = address;
  }

  static RaftPeer parse(String value) {
    int at = value.indexOf('@'), colon = value.lastIndexOf(':');
    if (at < 1 || colon <= at + 1 || colon == value.length() - 1)
      throw new IllegalArgumentException("raft peer must be nodeId@host:port: " + value);
    String node = value.substring(0, at).trim(), host = value.substring(at + 1, colon).trim();
    int port;
    try {
      port = Integer.parseInt(value.substring(colon + 1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("invalid raft peer port: " + value, e);
    }
    if (node.isEmpty() || host.isEmpty() || port < 1 || port > 65535)
      throw new IllegalArgumentException("invalid raft peer: " + value);
    return new RaftPeer(node, new InetSocketAddress(host, port));
  }

  static InetSocketAddress parseAddress(String value) {
    int colon = value.lastIndexOf(':');
    if (colon < 1 || colon == value.length() - 1)
      throw new IllegalArgumentException("raft bindAddress must be host:port: " + value);
    int port = Integer.parseInt(value.substring(colon + 1));
    if (port < 1 || port > 65535) throw new IllegalArgumentException("invalid raft bind port");
    return new InetSocketAddress(value.substring(0, colon), port);
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof RaftPeer && nodeId.equals(((RaftPeer) other).nodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeId);
  }

  @Override
  public String toString() {
    return nodeId + '@' + address.getHostString() + ':' + address.getPort();
  }
}
