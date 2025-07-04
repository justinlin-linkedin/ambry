/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.filecopy.MockFileCopyHandlerFactory;
import com.github.ambry.filecopy.MockNoOpFileCopyHandler;
import com.github.ambry.filetransfer.handler.FileCopyHandler;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class FileCopyThreadTest {

  public ClusterMap clusterMap;
  public FileCopyMetrics fileCopyMetrics;

  @Before
  public void setUp() throws IOException {
    clusterMap = new MockClusterMap(false, true, 1, 10, 3, false, false, null);
    fileCopyMetrics = new FileCopyMetrics(clusterMap.getMetricRegistry());
  }

  /**
   * Tests when fileCopyHandler is successful and onFileCopySuccess is called.
   * and onFileCopyFailure is not called
   */
  @Test
  public void testFileCopyThreadHandlerSuccess() {
    final Map<String, Integer> successFailCount = new HashMap<>();
    successFailCount.putIfAbsent("success", 0);
    successFailCount.putIfAbsent("fail", 0);
    FileCopyHandler fileCopyHandler = new MockFileCopyHandlerFactory().getFileCopyHandler();
    Thread fileCopyThreadThread = getThread(successFailCount, fileCopyHandler);

    fileCopyThreadThread.start();
    try {
      fileCopyThreadThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    assertEquals(1, successFailCount.get("success").intValue());
    assertEquals(0, successFailCount.get("fail").intValue());
  }

  /**
   * Tests when fileCopyHandler is failed and onFileCopyFailure is called.
   * and onFileCopySuccess is not called
   */
  @Test
  public void testFileCopyThreadHandlerFailure() {
    final Map<String, Integer> successFailCount = new HashMap<>();
    successFailCount.putIfAbsent("success", 0);
    successFailCount.putIfAbsent("fail", 0);
    FileCopyHandler fileCopyHandler = new MockFileCopyHandlerFactory().getFileCopyHandler();
    ((MockNoOpFileCopyHandler) fileCopyHandler).setException(new Exception());
    Thread fileCopyThreadThread = getThread(successFailCount, fileCopyHandler);

    fileCopyThreadThread.start();
    try {
      fileCopyThreadThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    assertEquals(0, successFailCount.get("success").intValue());
    assertEquals(1, successFailCount.get("fail").intValue());
  }

  private Thread getThread(Map<String, Integer> successFailCount, FileCopyHandler fileCopyHandler) {
    FileCopyStatusListener fileCopyStatusListener = new FileCopyStatusListener() {
      @Override
      public void onFileCopySuccess() {
        successFailCount.put("success", successFailCount.get("success") + 1);
      }

      @Override
      public ReplicaId getReplicaId() {
        MockClusterMap mockClusterMap = null;
        try {
          mockClusterMap = new MockClusterMap(false, true, 1, 10, 3, false, false, null);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        MockPartitionId partitionId =
            new MockPartitionId(1L, mockClusterMap.DEFAULT_PARTITION_CLASS, mockClusterMap.getDataNodes(), 0);
        return partitionId.getReplicaIds().get(0);
      }

      @Override
      public void onFileCopyFailure(Exception e) {
        successFailCount.put("fail", successFailCount.get("fail") + 1);
      }
    };

    FileCopyThread fileCopyThread = new FileCopyThread(fileCopyHandler, fileCopyStatusListener, fileCopyMetrics);
    Thread fileCopyThreadThread = new Thread(fileCopyThread);
    return fileCopyThreadThread;
  }
}
