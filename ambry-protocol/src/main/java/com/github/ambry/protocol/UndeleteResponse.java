/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 * Response of undelete rqeuest.
 */
public class UndeleteResponse extends Response {
  private static final short UNDELETE_RESPONSE_VERSION_1 = 1;
  public static final short INVALID_LIFE_VERSION = -1;

  private short lifeVersion;
  private static final int Life_Version_InBytes = Short.BYTES;

  /**
   * Constructs a {@link UndeleteResponse} with an {@link ServerErrorCode}.
   * @param correlationId correlationId of the undelete response.
   * @param clientId clientId of the undelete response.
   * @param error error code returned in this undelete response.
   */
  public UndeleteResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.UndeleteResponse, UNDELETE_RESPONSE_VERSION_1, correlationId, clientId, error);
    if (error == ServerErrorCode.No_Error || error == ServerErrorCode.Blob_Already_Undeleted) {
      throw new IllegalArgumentException("NoError is not a valid error code");
    }
    this.lifeVersion = INVALID_LIFE_VERSION;
  }

  /**
   * Constructs a {@link UndeleteResponse} with a valid lifeVersion. The error code will be set to {@link ServerErrorCode#No_Error}.
   * @param correlationId correlationId of the undelete response.
   * @param clientId clientId of the undelete response.
   * @param lifeVersion a valid lifeVersion to return to client.
   */
  public UndeleteResponse(int correlationId, String clientId, short lifeVersion) {
    super(RequestOrResponseType.UndeleteResponse, UNDELETE_RESPONSE_VERSION_1, correlationId, clientId,
        ServerErrorCode.No_Error);
    if (lifeVersion == INVALID_LIFE_VERSION) {
      throw new IllegalArgumentException("Invalid life version " + lifeVersion);
    }
    this.lifeVersion = lifeVersion;
  }

  /**
   * Constructs a {@link UndeleteResponse} with a valid lifeVersion. The error code will be set to {@link ServerErrorCode#No_Error}.
   * @param correlationId correlationId of the undelete response.
   * @param clientId clientId of the undelete response.
   * @param lifeVersion a valid lifeVersion to return to client.
   * @param error error code returned in this undelete response.
   */
  public UndeleteResponse(int correlationId, String clientId, short lifeVersion, ServerErrorCode error) {
    super(RequestOrResponseType.UndeleteResponse, UNDELETE_RESPONSE_VERSION_1, correlationId, clientId, error);
    this.lifeVersion = lifeVersion;
  }

  public static UndeleteResponse readFrom(DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.UndeleteResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId = stream.readShort();
    if (versionId != UNDELETE_RESPONSE_VERSION_1) {
      throw new IllegalArgumentException("Unknown version from stream " + versionId);
    }
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    short lifeVersion = stream.readShort();
    return new UndeleteResponse(correlationId, clientId, lifeVersion, error);
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeShort(lifeVersion);
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + (long) Life_Version_InBytes;
  }

  @Override
  public ByteBuf toProtobuf() {
    RequestOrResponseProto base = RequestOrResponseProto.newBuilder()
        .setType(RequestOrResponseProto.RequestOrResponseType.UndeleteResponse)
        .setCorrelationId(correlationId)
        .setVersionId(versionId)
        .setClientId(clientId)
        .build();
    UndeleteResponseProto response = UndeleteResponseProto.newBuilder()
        .setResponse(base)
        .setError(getError().ordinal())
        .setLiftVersion(lifeVersion)
        .build();
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

  public static UndeleteResponse readProtobufFrom(ByteBuf byteBuf, ClusterMap clusterMap) throws IOException {
    UndeleteResponseProto response = UndeleteResponseProto.parseFrom(byteBuf.nioBuffer());
    byteBuf.skipBytes(response.getSerializedSize());
    RequestOrResponseProto base = response.getResponse();
    return new UndeleteResponse(base.getCorrelationId(), base.getClientId(), (short) response.getLiftVersion(),
        ServerErrorCode.values()[response.getError()]);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("UndeleteResponse[");
    sb.append("ServerErrorCode=").append(getError());
    sb.append("LifeVersion=").append(lifeVersion);
    sb.append("]");
    return sb.toString();
  }

  /**
   * Return the lifeVersion.
   * @return the life version.
   */
  public short getLifeVersion() {
    return lifeVersion;
  }
}
