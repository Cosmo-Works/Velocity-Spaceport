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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class FrameSealTest {

  private final AtomicLong now = new AtomicLong(1_000_000);
  private final LocalStore store = new LocalStore();

  private static RouteTicket.Keys keys(String kid, int fill) {
    Map<String, byte[]> keys = new LinkedHashMap<>();
    byte[] key = new byte[32];
    java.util.Arrays.fill(key, (byte) fill);
    keys.put(kid, key);
    return new RouteTicket.Keys(keys);
  }

  private FrameSeal seal(RouteTicket.Keys keys) {
    return new FrameSeal(store, () -> keys, now::get);
  }

  private static final byte[] FRAME = {MessageBus.REQUEST, 1, 2, 3};

  @Test
  void sameKeyIsHeardOnce() {
    FrameSeal a = seal(keys("k1", 1));
    FrameSeal b = seal(keys("k1", 1));
    now.addAndGet(FrameSeal.MAX_AHEAD_MILLIS);
    byte[] sealed = a.seal("msg:all", FRAME);
    assertArrayEquals(FRAME, b.open("msg:all", sealed));
    assertNull(b.open("msg:all", sealed), "a replay is dropped");
    now.addAndGet(FrameSeal.MAX_AHEAD_MILLIS);
    assertArrayEquals(FRAME, b.open("msg:all", a.seal("msg:all", FRAME)), "a resend is new");
    assertEquals(1, b.rejected());
  }

  @Test
  void unsignedForgedOrMovedFramesAreDropped() {
    FrameSeal b = seal(keys("k1", 1));
    now.addAndGet(FrameSeal.MAX_AHEAD_MILLIS);
    assertNull(b.open("msg:all", FRAME), "unsigned, as an attacker on Redis would send");
    assertNull(b.open("msg:all", seal(keys("k1", 2)).seal("msg:all", FRAME)), "other key");
    assertNull(b.open("msg:all", seal(keys("k9", 1)).seal("msg:all", FRAME)), "unknown key id");
    assertNull(b.open("msg:p2", seal(keys("k1", 1)).seal("msg:p1", FRAME)), "other channel");
    byte[] tampered = seal(keys("k1", 1)).seal("msg:all", FRAME);
    tampered[tampered.length - 1] ^= 1;
    assertNull(b.open("msg:all", tampered));
    assertNull(b.open("msg:all", new byte[0]));
    assertEquals(6, b.rejected());
  }

  @Test
  void staleFramesAreDropped() {
    FrameSeal a = seal(keys("k1", 1));
    byte[] sealed = a.seal("msg:all", FRAME);
    now.addAndGet(FrameSeal.MAX_AGE_MILLIS + 1);
    assertNull(seal(keys("k1", 1)).open("msg:all", sealed));
  }

  @Test
  void restartCannotReplayEarlierFrames() {
    byte[] sealed = seal(keys("k1", 1)).seal("msg:all", FRAME);
    now.addAndGet(FrameSeal.MAX_AHEAD_MILLIS);
    assertNull(seal(keys("k1", 1)).open("msg:all", sealed));
  }

  @Test
  void framesFromTooFarAheadAreDropped() {
    FrameSeal b = seal(keys("k1", 1));
    now.addAndGet(FrameSeal.MAX_AHEAD_MILLIS);
    FrameSeal ahead = new FrameSeal(store, () -> keys("k1", 1),
        () -> now.get() + FrameSeal.MAX_AHEAD_MILLIS + 1);
    assertNull(b.open("msg:all", ahead.seal("msg:all", FRAME)));
  }

  @Test
  void withoutKeysNothingIsSentOrHeard() {
    FrameSeal none = new FrameSeal(store, () -> null, now::get);
    assertNull(none.seal("msg:all", FRAME));
    assertTrue(none.publish("msg:all", FRAME).isCompletedExceptionally());
    assertNull(none.open("msg:all", seal(keys("k1", 1)).seal("msg:all", FRAME)));
  }

  @Test
  void subscribersOnlySeeSignedFrames() {
    FrameSeal a = seal(keys("k1", 1));
    now.addAndGet(FrameSeal.MAX_AHEAD_MILLIS);
    List<byte[]> heard = new ArrayList<>();
    a.subscribe("msg:all", heard::add).join();
    store.publish("msg:all", FRAME).join();
    a.publish("msg:all", FRAME).join();
    assertEquals(1, heard.size());
    assertArrayEquals(FRAME, heard.get(0));
  }
}
