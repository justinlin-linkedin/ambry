/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import com.google.protobuf.CodedOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.DataInputStream;
import java.io.IOException;


/**
 * Representation of a response to an administration request.
 */
public class AdminResponse extends Response {
  private static final short ADMIN_RESPONSE_VERSION_V1 = 1;

  /**
   * Constructs a response.
   * @param correlationId an ID to help match responses to requests.
   * @param clientId the ID of the client.
   * @param error the {@link ServerErrorCode} for the request.
   */
  public AdminResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.AdminResponse, ADMIN_RESPONSE_VERSION_V1, correlationId, clientId, error);
  }

  /**
   * Reads from a stream and constructs an {@link AdminResponse}.
   * @param stream  the {@link DataInputStream} to read from.
   * @return {@link AdminResponse} that is deserialized from the {@code stream}.
   * @throws IOException if there is an I/O error reading from {@code stream}
   */
  public static AdminResponse readFrom(DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.AdminResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible (is " + type + ")");
    }
    Short versionId = stream.readShort();
    if (!versionId.equals(ADMIN_RESPONSE_VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for AdminResponse: " + versionId);
    }
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    // ignore version for now
    return new AdminResponse(correlationId, clientId, error);
  }

  @Override
  public ByteBuf toProtobuf() {
    RequestOrResponseProto base = RequestOrResponseProto.newBuilder()
        .setType(RequestOrResponseProto.RequestOrResponseType.AdminResponse)
        .setCorrelationId(correlationId)
        .setVersionId(versionId)
        .setClientId(clientId)
        .build();
    AdminResponseProto response =
        AdminResponseProto.newBuilder().setResponse(base).setError(getError().ordinal()).build();
    int size = response.getSerializedSize();

    ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.ioBuffer(size);
    try {
      int writerIndex = byteBuf.writerIndex();
      response.writeTo(CodedOutputStream.newInstance(byteBuf.nioBuffer()));
      byteBuf.writerIndex(writerIndex + size);
    } catch (IOException e) {

    }
    return byteBuf;
  }

  public static AdminResponse readProtobufFrom(ByteBuf byteBuf, ClusterMap clusterMap) throws IOException {
    AdminResponseProto response = AdminResponseProto.parseFrom(byteBuf.nioBuffer());
    byteBuf.skipBytes(response.getSerializedSize());
    RequestOrResponseProto base = response.getResponse();
    return new AdminResponse(base.getCorrelationId(), base.getClientId(),
        ServerErrorCode.values()[response.getError()]);
  }

  @Override
  public String toString() {
    return "AdminResponse[ClientId=" + clientId + ", CorrelationId=" + correlationId + ", Type=" + type
        + ", ServerError=" + getError() + "]";
  }
}
