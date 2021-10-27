/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.messageformat;

public enum StorageClass {
  REPLICATED(0, 0), RS_6_3(6, 3), RS_8_3(8, 3), RS_10_4(10, 4), RS_12_5(12, 5);

  private int numberOfDataChunks;
  private int numberOfParityChunks;

  StorageClass(int numberOfDataChunks, int numberOfParityChunks) {
    this.numberOfDataChunks = numberOfDataChunks;
    this.numberOfParityChunks = numberOfParityChunks;
  }

  public int getNumberOfDataChunks() {
    return this.numberOfDataChunks;
  }

  public int getNumberOfParityChunks() {
    return this.numberOfParityChunks;
  }
}
