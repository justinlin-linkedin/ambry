/**
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
package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.network.Send;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.Utils;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Response to GetRequest to fetch data
 */
public class GetResponse extends Response {

  protected long sentBytes = 0;
  private int bufferIndex = 0;
  protected ByteBuffer[] nioBuffers;
  private Send toSend = null;
  private int sendSizeInBufferToSend = 0;
  private InputStream stream = null;
  private final List<PartitionResponseInfo> partitionResponseInfoList;
  private int partitionResponseInfoSize;

  private static int Partition_Response_Info_List_Size = 4;
  static final short GET_RESPONSE_VERSION_V_1 = 1;
  static final short GET_RESPONSE_VERSION_V_2 = 2;
  static final short GET_RESPONSE_VERSION_V_3 = 3;
  static final short GET_RESPONSE_VERSION_V_4 = 4;
  static final short GET_RESPONSE_VERSION_V_5 = 5;

  static short CURRENT_VERSION = GET_RESPONSE_VERSION_V_5;

  public GetResponse(int correlationId, String clientId, List<PartitionResponseInfo> partitionResponseInfoList,
      Send send, ServerErrorCode error) {
    super(RequestOrResponseType.GetResponse, CURRENT_VERSION, correlationId, clientId, error);
    this.partitionResponseInfoList = partitionResponseInfoList;
    this.partitionResponseInfoSize = 0;
    for (PartitionResponseInfo partitionResponseInfo : partitionResponseInfoList) {
      this.partitionResponseInfoSize += partitionResponseInfo.sizeInBytes();
    }
    this.toSend = send;
  }

  public GetResponse(int correlationId, String clientId, List<PartitionResponseInfo> partitionResponseInfoList,
      InputStream stream, ServerErrorCode error) {
    super(RequestOrResponseType.GetResponse, CURRENT_VERSION, correlationId, clientId, error);
    this.partitionResponseInfoList = partitionResponseInfoList;
    this.partitionResponseInfoSize = 0;
    for (PartitionResponseInfo partitionResponseInfo : partitionResponseInfoList) {
      this.partitionResponseInfoSize += partitionResponseInfo.sizeInBytes();
    }
    this.stream = stream;
  }

  public GetResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.GetResponse, CURRENT_VERSION, correlationId, clientId, error);
    this.partitionResponseInfoList = null;
    this.partitionResponseInfoSize = 0;
  }

  public InputStream getInputStream() {
    return stream;
  }

  public List<PartitionResponseInfo> getPartitionResponseInfoList() {
    return partitionResponseInfoList;
  }

  public static GetResponse readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    short typeval = stream.readShort();
    RequestOrResponseType type = RequestOrResponseType.values()[typeval];
    if (type != RequestOrResponseType.GetResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId = stream.readShort();
    // ignore version for now
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];

    if (error != ServerErrorCode.No_Error) {
      return new GetResponse(correlationId, clientId, error);
    } else {
      int partitionResponseInfoCount = stream.readInt();
      ArrayList<PartitionResponseInfo> partitionResponseInfoList =
          new ArrayList<PartitionResponseInfo>(partitionResponseInfoCount);
      for (int i = 0; i < partitionResponseInfoCount; i++) {
        PartitionResponseInfo partitionResponseInfo = PartitionResponseInfo.readFrom(stream, map, versionId);
        partitionResponseInfoList.add(partitionResponseInfo);
      }
      return new GetResponse(correlationId, clientId, partitionResponseInfoList, stream, error);
    }
  }

  /**
   * A private method shared by {@link GetResponse#writeTo(WritableByteChannel)} and
   * {@link GetResponse#writeTo(AsyncWritableChannel, Callback)}.
   * This method allocate bufferToSend and write metadata to it if bufferToSend is null.
   */
  @Override
  protected void prepareBuffer() {
    bufferToSend = PooledByteBufAllocator.DEFAULT.ioBuffer(
        (int) super.sizeInBytes() + (Partition_Response_Info_List_Size + partitionResponseInfoSize));
    writeHeader();
    if (partitionResponseInfoList != null) {
      bufferToSend.writeInt(partitionResponseInfoList.size());
      for (PartitionResponseInfo partitionResponseInfo : partitionResponseInfoList) {
        partitionResponseInfo.writeTo(bufferToSend);
      }
    }
    if (toSend != null) {
      ByteBuf toSendContent = toSend.content();
      if (toSendContent != null) {
        // Since this composite blob will be a readonly blob, we don't really care about if it's allocated
        // on a direct memory or not.
        CompositeByteBuf compositeByteBuf = bufferToSend.alloc().compositeDirectBuffer();
        int maxNumComponent = 1 + toSendContent.nioBufferCount();
        compositeByteBuf.addComponent(true, bufferToSend);
        compositeByteBuf.addComponent(true, toSendContent);
        bufferToSend = compositeByteBuf;
        sendSizeInBufferToSend = toSendContent.readableBytes();
        toSend = null;
      }
    }
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      prepareBuffer();
    }
    if (nioBuffers == null) {
      nioBuffers = bufferToSend.nioBuffers();
    }
    long totalWritten = 0;
    if (bufferToSend.readableBytes() != 0) {
      int currentWritten = -1;
      while (bufferIndex < nioBuffers.length && currentWritten != 0) {
        currentWritten = -1;
        ByteBuffer byteBuffer = nioBuffers[bufferIndex];
        if (!byteBuffer.hasRemaining()) {
          // some bytebuffers are zero length, ignore those bytebuffers.
          bufferIndex++;
        } else {
          currentWritten = channel.write(byteBuffer);
          totalWritten += currentWritten;
        }
      }
      bufferToSend.skipBytes((int) totalWritten);
      sentBytes += totalWritten;
    }
    if (sentBytes >= bufferToSend.readableBytes() && toSend != null && !toSend.isSendComplete()) {
      long written = toSend.writeTo(channel);
      totalWritten += written;
      sentBytes += written;
    }
    return totalWritten;
  }

  @Override
  public void writeTo(AsyncWritableChannel channel, Callback<Long> callback) {
    if (bufferToSend == null) {
      prepareBuffer();
    }
    channel.write(bufferToSend, callback);
    if (toSend != null) {
      toSend.writeTo(channel, callback);
    }
  }

  /**
   * Override the release method from {@link RequestOrResponse}. When the {@link #prepareBuffer()} is not invoked, there
   * will be {@link #toSend}'s content waiting for releasing.
   * @return
   */
  @Override
  public boolean release() {
    if (bufferToSend != null) {
      ReferenceCountUtil.safeRelease(bufferToSend);
      bufferToSend = null;
    }
    if (toSend != null) {
      ReferenceCountUtil.safeRelease(toSend);
      toSend = null;
    }
    return false;
  }

  @Override
  public boolean isSendComplete() {
    return sizeInBytes() == sentBytes;
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + (Partition_Response_Info_List_Size + partitionResponseInfoSize) + ((toSend == null)
        ? sendSizeInBufferToSend : toSend.sizeInBytes());
  }

  @Override
  public ByteBuf toProtobuf() {
    RequestOrResponseProto base = RequestOrResponseProto.newBuilder()
        .setType(RequestOrResponseProto.RequestOrResponseType.GetResponse)
        .setCorrelationId(correlationId)
        .setVersionId(versionId)
        .setClientId(clientId)
        .build();
    GetResponseProto.Builder responseBuilder =
        GetResponseProto.newBuilder().setResponse(base).setError(getError().ordinal());
    for (PartitionResponseInfo responseInfo : partitionResponseInfoList) {
      PartitionResponseInfoProto.Builder infoBuilder = PartitionResponseInfoProto.newBuilder();
      infoBuilder.setPartitionId(ByteString.copyFrom(responseInfo.getPartition().getBytes()))
          .setError(responseInfo.getErrorCode().ordinal());
      for (MessageInfo messageInfo : responseInfo.getMessageInfoList()) {
        infoBuilder.addMessageInfoList(MessageInfoProto.newBuilder()
            .setStoreKey(ByteString.copyFrom(messageInfo.getStoreKey().toBytes()))
            .setSize(messageInfo.getSize())
            .setExpirationTimeInMs(messageInfo.getExpirationTimeInMs())
            .setIsDeleted(messageInfo.isDeleted())
            .setIsTtlUpdated(messageInfo.isTtlUpdated())
            .setIsUndeleted(messageInfo.isUndeleted())
            .setCrc(messageInfo.getCrc())
            .setAccountId(messageInfo.getAccountId())
            .setContainerId(messageInfo.getContainerId())
            .setLifeVersion(messageInfo.getLifeVersion())
            .build());
      }
      for (MessageMetadata mm : responseInfo.getMessageMetadataList()) {
        infoBuilder.addEncryptionKeys(ByteString.copyFrom(mm.getEncryptionKey()));
      }
      responseBuilder.addPartitionResponseInfoList(infoBuilder.build());
    }
    GetResponseProto response = responseBuilder.build();
    int size = response.getSerializedSize();
    ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.ioBuffer(size);
    try {
      int writerIndex = byteBuf.writerIndex();
      response.writeTo(CodedOutputStream.newInstance(byteBuf.nioBuffer()));
      byteBuf.writerIndex(writerIndex + size);
    } catch (IOException e) {

    }
    if (toSend != null) {
      ByteBuf toSendContent = toSend.content();
      if (toSendContent != null) {
        // Since this composite blob will be a readonly blob, we don't really care about if it's allocated
        // on a direct memory or not.
        CompositeByteBuf compositeByteBuf = byteBuf.alloc().compositeDirectBuffer();
        int maxNumComponent = 1 + toSendContent.nioBufferCount();
        compositeByteBuf.addComponent(true, bufferToSend);
        compositeByteBuf.addComponent(true, toSendContent);
        byteBuf = compositeByteBuf;
      }
    }
    return byteBuf;
  }

  public static GetResponse readProtobufFrom(ByteBuf byteBuf, ClusterMap clusterMap) throws IOException {
    GetResponseProto response = GetResponseProto.parseFrom(byteBuf.nioBuffer());
    byteBuf.skipBytes(response.getSerializedSize());
    RequestOrResponseProto base = response.getResponse();
    ServerErrorCode errorCode = ServerErrorCode.values()[response.getError()];
    if (errorCode != ServerErrorCode.No_Error) {
      return new GetResponse(base.getCorrelationId(), base.getClientId(), errorCode);
    } else {
      int partitionResponseInfoCount = response.getPartitionResponseInfoListCount();
      ArrayList<PartitionResponseInfo> partitionResponseInfoList =
          new ArrayList<PartitionResponseInfo>(partitionResponseInfoCount);
      for (PartitionResponseInfoProto info : response.getPartitionResponseInfoListList()) {
        List<MessageMetadata> messageMetadataList = info.getEncryptionKeysList()
            .stream()
            .map(bs -> new MessageMetadata(ByteBuffer.wrap(bs.toByteArray())))
            .collect(Collectors.toList());
        List<MessageInfo> messageInfoList = new ArrayList<>(info.getMessageInfoListCount());
        for (MessageInfoProto mp : info.getMessageInfoListList()) {
          BlobId blobId = new BlobId(mp.getStoreKey().toString(), clusterMap);
          messageInfoList.add(
              new MessageInfo.Builder(blobId, mp.getSize(), (short) mp.getAccountId(), (short) mp.getContainerId(),
                  mp.getOperationTimeMs()).isDeleted(mp.getIsDeleted())
                  .isTtlUpdated(mp.getIsTtlUpdated())
                  .isUndeleted(mp.getIsUndeleted())
                  .crc(mp.getCrc())
                  .lifeVersion((short) mp.getLifeVersion())
                  .build());
        }
        errorCode = ServerErrorCode.values()[info.getError()];
        PartitionResponseInfo partitionResponseInfo = null;
        PartitionId partitionId =
            clusterMap.getPartitionIdFromStream(new ByteArrayInputStream(info.getPartitionId().toByteArray()));
        if (errorCode == ServerErrorCode.No_Error) {
          partitionResponseInfo = new PartitionResponseInfo(partitionId, messageInfoList, messageMetadataList);
        } else {
          partitionResponseInfo = new PartitionResponseInfo(partitionId, errorCode);
        }
        partitionResponseInfoList.add(partitionResponseInfo);
      }
      byteBuf.skipBytes(response.getSerializedSize());
      return new GetResponse(base.getCorrelationId(), base.getClientId(), partitionResponseInfoList,
          new NettyByteBufDataInputStream(byteBuf), errorCode);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("GetResponse[");
    if (toSend != null) {
      sb.append("SizeToSend=").append(toSend.sizeInBytes());
    }
    sb.append(" ServerErrorCode=").append(getError());
    if (partitionResponseInfoList != null) {
      sb.append(" PartitionResponseInfoList=").append(partitionResponseInfoList);
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * @return the current version in which new GetResponse objects are created.
   */
  static short getCurrentVersion() {
    return CURRENT_VERSION;
  }
}
