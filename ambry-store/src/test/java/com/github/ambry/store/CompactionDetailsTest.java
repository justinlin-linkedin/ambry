/*
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.store;

import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link CompactionDetails}.
 */
public class CompactionDetailsTest {

  /**
   * Tests the serialization and deserialization of {@link CompactionDetails} and verifies eqaulity of the original and
   * deserialized forms.
   * @throws IOException
   */
  @Test
  public void serDeTest() throws IOException {
    for (short version : new short[]{CompactionDetails.VERSION_0, CompactionDetails.VERSION_1}) {
      CompactionDetails.CURRENT_VERSION = version;
      int segmentCount = TestUtils.RANDOM.nextInt(10) + 1;
      List<LogSegmentName> segmentsUnderCompaction = new ArrayList<>();
      for (int i = 0; i < segmentCount; i++) {
        LogSegmentName segmentName = StoreTestUtils.getRandomLogSegmentName(segmentsUnderCompaction);
        segmentsUnderCompaction.add(segmentName);
      }
      long referenceTime = SystemTime.getInstance().milliseconds();
      for (boolean isFullRange : new boolean[]{false, true}) {
        CompactionDetails details = new CompactionDetails(referenceTime, segmentsUnderCompaction, null, isFullRange);
        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(details.toBytes()));
        verifyEquality(details, CompactionDetails.fromBytes(stream));
      }
    }
  }

  @Test
  public void serDeBackwardCompatibilityTest() throws IOException {
    CompactionDetails.CURRENT_VERSION = CompactionDetails.VERSION_0;
    int segmentCount = TestUtils.RANDOM.nextInt(10) + 1;
    List<LogSegmentName> segmentsUnderCompaction = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      LogSegmentName segmentName = StoreTestUtils.getRandomLogSegmentName(segmentsUnderCompaction);
      segmentsUnderCompaction.add(segmentName);
    }
    long referenceTime = SystemTime.getInstance().milliseconds();
    CompactionDetails details = new CompactionDetails(referenceTime, segmentsUnderCompaction, null);
    DataInputStream stream = new DataInputStream(new ByteArrayInputStream(details.toBytes()));

    CompactionDetails.CURRENT_VERSION = CompactionDetails.VERSION_1;
    verifyEquality(details, CompactionDetails.fromBytes(stream));
  }

  /**
   * Tests for bad arguments while constructing {@link CompactionDetails}.
   * @throws Exception
   */
  @Test
  public void badInputTest() throws Exception {
    List<LogSegmentName> segmentsUnderCompaction =
        Collections.singletonList(LogSegmentName.generateFirstSegmentName(true));

    // details contains no segments
    try {
      new CompactionDetails(1, Collections.emptyList(), null);
      fail("Should have failed because there were no log segments to compact");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // details has a negative ref time.
    try {
      new CompactionDetails(-1, segmentsUnderCompaction, null);
      fail("Should have failed because reference time is < 0");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // 0 ref time is ok.
    new CompactionDetails(0, segmentsUnderCompaction, null);
  }

  /**
   * Verifies that two {@link CompactionDetails} instances are equal.
   * @param original the expected {@link CompactionDetails}.
   * @param toCheck the {@link CompactionDetails} that needs to be checked.
   */
  private void verifyEquality(CompactionDetails original, CompactionDetails toCheck) {
    assertEquals("Reference time does not match", original.getReferenceTimeMs(), toCheck.getReferenceTimeMs());
    assertEquals("Segments under compaction don't match", original.getLogSegmentsUnderCompaction(),
        toCheck.getLogSegmentsUnderCompaction());
    if (CompactionDetails.CURRENT_VERSION == CompactionDetails.VERSION_1) {
      assertEquals("IsFullRange does not match", original.isFullRange(), toCheck.isFullRange());
    } else {
      assertFalse("IsFullRange does not match", toCheck.isFullRange());
    }
  }
}
