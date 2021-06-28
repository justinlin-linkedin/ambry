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
package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Method;


public enum RequestResponseCodec {
  BINARY("application/octet-stream") {
    ByteBuf encode(RequestOrResponse requestOrResponse) {
      requestOrResponse.prepareBuffer();
      return requestOrResponse.content();
    }

    <T> T decode(ByteBuf byteBuf, Class<T> clazz, ClusterMap clusterMap) throws IOException {
      try {
        Method method = clazz.getMethod("readFrom", DataInputStream.class, ClusterMap.class);
        Object object = method.invoke(null, new NettyByteBufDataInputStream(byteBuf), clusterMap);
        if (object.getClass().equals(clazz)) {
          return (T) object;
        } else {
          throw new IOException("Something wrong with the readFrom method");
        }
      } catch (Exception e) {
        if (e instanceof IOException) {
          throw (IOException) e;
        } else {
          throw new IOException("Unknown exception: ", e);
        }
      }
    }
  }, PROTOBUF("application/protobuf") {
    ByteBuf encode(RequestOrResponse requestOrResponse) {
      return requestOrResponse.toProtobuf();
    }

    <T> T decode(ByteBuf byteBuf, Class<T> clazz, ClusterMap clusterMap) throws IOException {
      try {
        Method method = clazz.getMethod("readProtobufFrom", ByteBuf.class, ClusterMap.class);
        Object object = method.invoke(null, byteBuf, clusterMap);
        if (object.getClass().equals(clazz)) {
          return (T) object;
        } else {
          throw new IOException("Something wrong with the readFrom method");
        }
      } catch (Exception e) {
        if (e instanceof IOException) {
          throw (IOException) e;
        } else {
          throw new IOException("Unknown exception: ", e);
        }
      }
    }
  };

  String httpContentEncoding;

  RequestResponseCodec(String httpContentEncoding) {
    this.httpContentEncoding = httpContentEncoding;
  }

  String httpContentEncodingValue() {
    return httpContentEncoding;
  }

  static RequestResponseCodec fromHttpContentEncoding(String httpContentEncoding) {
    if (httpContentEncoding.equals(BINARY.httpContentEncoding)) {
      return BINARY;
    }
    if (httpContentEncoding.equals(PROTOBUF.httpContentEncoding)) {
      return PROTOBUF;
    }
    throw new IllegalArgumentException("Unknown http content encoding value: " + httpContentEncoding);
  }

  abstract ByteBuf encode(RequestOrResponse object);

  abstract <T> T decode(ByteBuf byteBuf, Class<T> clazz, ClusterMap clusterMap) throws IOException;
}
