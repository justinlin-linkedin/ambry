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

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.DataInputStream;
import java.io.IOException;


/**
 *  An admin request used to determine if the peers of a storage node have caught up to it.
 */
public class CatchupStatusAdminRequest extends AdminRequest {
  private static final short VERSION_V1 = 1;
  private static final short VERSION_V2 = 2;

  private final long acceptableLagInBytes;
  private final short numReplicasCaughtUpPerPartition;
  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs a {@link CatchupStatusAdminRequest}.
   * @param stream the stream to read from
   * @param adminRequest the {@link AdminRequest} that contains some necessary headers.
   * @return the {@link CatchupStatusAdminRequest} constructed from the {@code stream}.
   * @throws IOException if there is any problem reading from the stream
   */
  public static CatchupStatusAdminRequest readFrom(DataInputStream stream, AdminRequest adminRequest)
      throws IOException {
    Short versionId = stream.readShort();
    long acceptableLagInBytes;
    short numReplicasCaughtUpPerPartition = Short.MAX_VALUE;
    switch (versionId) {
      case VERSION_V1:
        acceptableLagInBytes = stream.readLong();
        break;
      case VERSION_V2:
        acceptableLagInBytes = stream.readLong();
        numReplicasCaughtUpPerPartition = stream.readShort();
        break;
      default:
        throw new IllegalStateException("Unrecognized version for CatchupStatusAdminRequest: " + versionId);
    }
    return new CatchupStatusAdminRequest(acceptableLagInBytes, numReplicasCaughtUpPerPartition, adminRequest);
  }

  /**
   * Construct a CatchupStatusAdminRequest
   * @param acceptableLagInBytes the number of bytes that the remote can lag by which is considered OK.
   * @param numReplicasCaughtUpPerPartition the number of replicas that have to be within {@code acceptableLagInBytes}
   *                                        (per partition). The min of this value or the total count of replicas - 1 is
   *                                        considered.
   * @param adminRequest the {@link AdminRequest} that contains common admin request related information.
   */
  public CatchupStatusAdminRequest(long acceptableLagInBytes, short numReplicasCaughtUpPerPartition,
      AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.CatchupStatus, adminRequest.getPartitionId(), adminRequest.getCorrelationId(),
        adminRequest.getClientId());
    this.acceptableLagInBytes = acceptableLagInBytes;
    this.numReplicasCaughtUpPerPartition = numReplicasCaughtUpPerPartition;
    // parent size + version size + acceptableLagInBytes size + numReplicasCaughtUpPerPartition size
    sizeInBytes = super.sizeInBytes() + Short.BYTES + Long.BYTES + Short.BYTES;
  }

  /**
   * @return the number of bytes that the remote can lag by which is considered OK.
   */
  public long getAcceptableLagInBytes() {
    return acceptableLagInBytes;
  }

  /**
   * @return the least number of replicas that have to be within {@link #getAcceptableLagInBytes()} for each partition.
   */
  public short getNumReplicasCaughtUpPerPartition() {
    return numReplicasCaughtUpPerPartition;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public ByteBuf toProtobuf() {
    RequestOrResponseProto base = RequestOrResponseProto.newBuilder()
        .setType(RequestOrResponseProto.RequestOrResponseType.AdminRequest)
        .setCorrelationId(correlationId)
        .setVersionId(versionId)
        .setClientId(clientId)
        .build();
    AdminRequestProto.Builder adminBaseBuilder =
        AdminRequestProto.newBuilder().setRequest(base).setType(AdminRequestOrResponseType.CatchupStatus.ordinal());
    if (getPartitionId() != null) {
      adminBaseBuilder.setPartitionId(ByteString.copyFrom(getPartitionId().getBytes()));
    }
    AdminRequestProto adminBase = adminBaseBuilder.build();
    CatchupStatusAdminRequestProto request = CatchupStatusAdminRequestProto.newBuilder()
        .setAcceptableLagInBytes(acceptableLagInBytes)
        .setNumReplicasCaughtUpPerPartition(numReplicasCaughtUpPerPartition)
        .build();
    int size = adminBase.getSerializedSize() + request.getSerializedSize();
    ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.ioBuffer(size);
    try {
      int writerIndex = byteBuf.writerIndex();
      adminBase.writeTo(CodedOutputStream.newInstance(byteBuf.nioBuffer()));
      byteBuf.writerIndex(writerIndex + adminBase.getSerializedSize());
      request.writeTo(CodedOutputStream.newInstance(byteBuf.nioBuffer()));
      byteBuf.writerIndex(writerIndex + request.getSerializedSize());
    } catch (IOException e) {

    }
    return byteBuf;
  }

  public static CatchupStatusAdminRequest readProtobufFrom(ByteBuf byteBuf, AdminRequest adminRequest)
      throws IOException {
    CatchupStatusAdminRequestProto request = CatchupStatusAdminRequestProto.parseFrom(byteBuf.nioBuffer());
    byteBuf.skipBytes(request.getSerializedSize());

    return new CatchupStatusAdminRequest(request.getAcceptableLagInBytes(),
        (short) request.getNumReplicasCaughtUpPerPartition(), adminRequest);
  }

  @Override
  public String toString() {
    return "CatchupStatusAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId
        + ", AcceptableLagInBytes=" + acceptableLagInBytes + ", PartitionId=" + getPartitionId() + "]";
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeShort(VERSION_V2);
    bufferToSend.writeLong(acceptableLagInBytes);
    bufferToSend.writeShort(numReplicasCaughtUpPerPartition);
  }
}
