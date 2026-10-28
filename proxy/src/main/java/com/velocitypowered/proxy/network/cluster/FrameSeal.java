/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.network.cluster;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Signs every frame proxies send each other over the store's pub/sub with the network key, and
 * drops frames that are unsigned, wrongly signed, stale or replayed. Anyone who can reach Redis
 * can publish; only a proxy holding the key can be heard. Without keys nothing is sent or heard.
 */
final class FrameSeal {

  static final byte SEALED = 0x53;
  /** How far a frame's clock may be from ours; a resend is sealed again, so this stays short. */
  static final long MAX_AGE_MILLIS = 30_000;
  /** Keeps a bus MAC apart from a transfer ticket MAC made with the same key. */
  private static final byte[] LABEL = "spaceport-bus\0".getBytes(StandardCharsets.UTF_8);
  private static final int MAC_BYTES = 32;

  private final NetworkStore store;
  private final Supplier<RouteTicket.@Nullable Keys> keys;
  private final LongSupplier clock;
  /** Longer than any frame stays fresh (up to MAX_AGE in the past and in the future). */
  private final Cache<ByteBuffer, Boolean> seen = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofMillis(MAX_AGE_MILLIS * 3))
      .build();
  /** Frames sealed before we started are not in {@link #seen}, so a restart cannot replay them. */
  private final long started;
  private final LongAdder rejected = new LongAdder();

  FrameSeal(NetworkStore store, Supplier<RouteTicket.@Nullable Keys> keys, LongSupplier clock) {
    this.store = store;
    this.keys = keys;
    this.clock = clock;
    this.started = clock.getAsLong();
  }

  /** Frames dropped as unsigned, forged, stale or replayed. */
  long rejected() {
    return rejected.sum();
  }

  CompletableFuture<Void> publish(String channel, byte[] frame) {
    byte[] sealed = seal(channel, frame);
    if (sealed == null) {
      return CompletableFuture.failedFuture(new IllegalStateException("no network key"));
    }
    return store.publish(channel, sealed);
  }

  CompletableFuture<Void> subscribe(String channel, Consumer<byte[]> handler) {
    return store.subscribe(channel, payload -> {
      byte[] frame = open(channel, payload);
      if (frame != null) {
        handler.accept(frame);
      }
    });
  }

  byte @Nullable [] seal(String channel, byte[] frame) {
    RouteTicket.Keys k = keys.get();
    if (k == null) {
      return null;
    }
    String kid = k.currentId();
    long now = clock.getAsLong();
    byte[] mac = k.mac(kid, signed(channel, now, frame));
    return MessageBus.encode(out -> {
      out.writeByte(SEALED);
      out.writeUTF(kid);
      out.writeLong(now);
      out.write(mac);
      out.write(frame);
    });
  }

  byte @Nullable [] open(String channel, byte[] payload) {
    byte[] frame = verify(channel, payload);
    if (frame == null) {
      rejected.increment();
    }
    return frame;
  }

  private byte @Nullable [] verify(String channel, byte[] payload) {
    RouteTicket.Keys k = keys.get();
    if (k == null || payload.length == 0 || payload[0] != SEALED) {
      return null;
    }
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
      in.readByte();
      String kid = in.readUTF();
      long sent = in.readLong();
      byte[] mac = in.readNBytes(MAC_BYTES);
      byte[] frame = in.readAllBytes();
      byte[] expected = k.mac(kid, signed(channel, sent, frame));
      if (mac.length != MAC_BYTES || expected == null || !MessageDigest.isEqual(mac, expected)) {
        return null;
      }
      if (sent < started || Math.abs(clock.getAsLong() - sent) > MAX_AGE_MILLIS
          || seen.asMap().putIfAbsent(ByteBuffer.wrap(mac), Boolean.TRUE) != null) {
        return null;
      }
      return frame;
    } catch (IOException e) {
      return null;
    }
  }

  private static byte[] signed(String channel, long sent, byte[] frame) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(frame.length + 64);
      DataOutputStream out = new DataOutputStream(bytes);
      out.write(LABEL);
      out.writeUTF(channel);
      out.writeLong(sent);
      out.write(frame);
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }
}
