/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.github.ambry.account.Container;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.FutureUtils;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ChunkInfo;
import com.github.ambry.router.PutBlobOptions;
import com.github.ambry.router.PutBlobOptionsBuilder;
import com.github.ambry.router.Router;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.commons.FutureUtils.*;
import static com.github.ambry.frontend.FrontendUtils.*;


/**
 * Handler for post blob requests. The following request types are handled by {@link PostBlobHandler}:
 * <h2>Direct uploads</h2>
 * Direct upload requests treat the body of the request as the content to upload to Ambry. The request path should be
 * "/". In these requests, the blob properties and user metadata are supplied as headers. See
 * {@link RestUtils#buildBlobProperties(Map)} and {@link RestUtils#buildBlobProperties(Map)} for more details.
 * <h2>Stitched uploads</h2>
 * Stitched upload requests allow clients to stitch together previously uploaded data chunks into a single logical blob.
 * The request path should be "/stitch" ({@link Operations#STITCH}). This request accepts the same headers as direct
 * upload requests for supplying the blob properties and user metadata of the stitched blob, but, instead of the actual
 * blob content, accepts a UTF-8 JSON object that includes the signed IDs for the chunks to stitch.
 * <h3>Request body format</h3>
 * The body of the request should be a JSON object that conforms to the format described in {@link StitchRequestSerDe}.
 */
class PostBlobHandler {
  /**
   * Key to represent the time at which a blob will expire in ms. Used within the metadata map in signed IDs.
   */
  static final String EXPIRATION_TIME_MS_KEY = "et";
  private static final Logger LOGGER = LoggerFactory.getLogger(PostBlobHandler.class);
  private final SecurityService securityService;
  private final IdConverter idConverter;
  private final IdSigningService idSigningService;
  private final Router router;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final Time time;
  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;
  private final String clusterName;
  private final QuotaManager quotaManager;

  /**
   * Constructs a handler for handling requests for uploading or stitching blobs.
   * @param securityService the {@link SecurityService} to use.
   * @param idConverter the {@link IdConverter} to use.
   * @param idSigningService the {@link IdSigningService} to use.
   * @param router the {@link Router} to use.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   * @param time the {@link Time} instance to use.
   * @param frontendConfig the {@link FrontendConfig} to use.
   * @param frontendMetrics {@link FrontendMetrics} instance where metrics should be recorded.
   * @param clusterName the name of the storage cluster that the router communicates with
   * @param quotaManager {@link QuotaManager} instance to charge against quota for each chunk.
   */
  PostBlobHandler(SecurityService securityService, IdConverter idConverter, IdSigningService idSigningService,
      Router router, AccountAndContainerInjector accountAndContainerInjector, Time time, FrontendConfig frontendConfig,
      FrontendMetrics frontendMetrics, String clusterName, QuotaManager quotaManager) {
    this.securityService = securityService;
    this.idConverter = idConverter;
    this.idSigningService = idSigningService;
    this.router = router;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.time = time;
    this.frontendConfig = frontendConfig;
    this.frontendMetrics = frontendMetrics;
    this.clusterName = clusterName;
    this.quotaManager = quotaManager;
  }

  /**
   * Asynchronously post a blob.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback) {
    restRequest.getMetricsTracker()
        .injectMetrics(frontendMetrics.postBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false));

    String context = restRequest.getUri();
    AtomicReference<BlobInfo> blobInfoRef = new AtomicReference<>();
    AtomicReference<String> blobIdRef = new AtomicReference<>();
    CompletableFuture<PutBlobOptions> putBlobOptionsFuture =
        FutureUtils.fromCallable(() -> getBlobInfoFromRequest(restRequest, restResponseChannel))
            .thenAccept(blobInfo -> blobInfoRef.set(blobInfo))
            .thenCompose(v -> buildContextOnFuture(() -> securityService.processRequest(restRequest),
                frontendMetrics.postSecurityProcessRequestMetrics, context, LOGGER))
            .thenCompose(v -> buildContextOnFuture(() -> securityService.postProcessRequest(restRequest),
                frontendMetrics.postSecurityPostProcessRequestMetrics, context, LOGGER))
            .thenCompose(v -> FutureUtils.fromCallable(() -> getPutBlobOptionsFromRequest(restRequest)));
    CompletableFuture<String> blobIdFuture;
    if (RestUtils.getRequestPath(restRequest).matchesOperation(Operations.STITCH)) {
      blobIdFuture = putBlobOptionsFuture.thenCompose(v -> buildContextOnFuture(() -> {
        RetainingAsyncWritableChannel channel =
            new RetainingAsyncWritableChannel(frontendConfig.maxJsonRequestSizeBytes);
        return FutureUtils.replaceReturnValue(restRequest.readInto(channel), channel);
      }, frontendMetrics.postReadStitchRequestMetrics, context, LOGGER).thenCompose(channel -> FutureUtils.fromCallable(
              () -> getChunksToStitch(restResponseChannel, blobInfoRef.get().getBlobProperties(),
                  readJsonFromChannel(channel))))
          .thenCompose(chunkInfos -> buildContextOnFuture(
              () -> router.stitchBlob(blobInfoRef.get().getBlobProperties(), blobInfoRef.get().getUserMetadata(),
                  chunkInfos, QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, false)),
              frontendMetrics.postRouterStitchBlobMetrics, context, LOGGER)));
    } else {
      blobIdFuture = putBlobOptionsFuture.thenCompose(putOptions -> buildContextOnFuture(
          () -> router.putBlob(blobInfoRef.get().getBlobProperties(), blobInfoRef.get().getUserMetadata(), restRequest,
              putOptions, QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, true)),
          frontendMetrics.postRouterPutBlobMetrics, context, LOGGER));
    }
    blobIdFuture.thenAccept(blobId -> blobIdRef.set(blobId))
        .thenCompose(v -> FutureUtils.fromCallable(() -> {
          setSignedIdMetadataAndBlobSize(restRequest, restResponseChannel, blobInfoRef.get().getBlobProperties());
          return null;
        }))
        .thenCompose(v -> buildContextOnFuture(() -> idConverter.convert(restRequest, blobIdRef.get()),
            frontendMetrics.postIdConversionMetrics, context, LOGGER))
        .thenAccept(convertedBlobId -> restResponseChannel.setHeader(RestUtils.Headers.LOCATION, convertedBlobId))
        .thenCompose(v -> buildContextOnFuture(
            () -> securityService.processResponse(restRequest, restResponseChannel, blobInfoRef.get()),
            frontendMetrics.postSecurityProcessResponseMetrics, context, LOGGER))
        .whenComplete((r, e) -> {
          if (e != null) {
            callback.onCompletion(null, Utils.extractFutureExceptionCause(e));
          } else {
            callback.onCompletion(r, null);
          }
        });
  }

  private BlobInfo getBlobInfoFromRequest(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    long propsBuildStartTime = System.currentTimeMillis();
    accountAndContainerInjector.injectAccountAndContainerForPostRequest(restRequest,
        frontendMetrics.postBlobMetricsGroup);
    BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest.getArgs());
    Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
    if (blobProperties.getTimeToLiveInSeconds() + TimeUnit.MILLISECONDS.toSeconds(blobProperties.getCreationTimeInMs())
        > Integer.MAX_VALUE) {
      LOGGER.debug("TTL set to very large value in POST request with BlobProperties {}", blobProperties);
      frontendMetrics.ttlTooLargeError.inc();
    } else if (container.isTtlRequired() && (blobProperties.getTimeToLiveInSeconds() == Utils.Infinite_Time
        || blobProperties.getTimeToLiveInSeconds() > frontendConfig.maxAcceptableTtlSecsIfTtlRequired)) {
      String descriptor = RestUtils.getAccountFromArgs(restRequest.getArgs()).getName() + ":" + container.getName();
      if (frontendConfig.failIfTtlRequiredButNotProvided) {
        throw new RestServiceException(
            "TTL < " + frontendConfig.maxAcceptableTtlSecsIfTtlRequired + " is required for upload to " + descriptor,
            RestServiceErrorCode.InvalidArgs);
      } else {
        LOGGER.debug("{} attempted an upload with ttl {} to {}", blobProperties.getServiceId(),
            blobProperties.getTimeToLiveInSeconds(), descriptor);
        frontendMetrics.ttlNotCompliantError.inc();
        restResponseChannel.setHeader(RestUtils.Headers.NON_COMPLIANCE_WARNING,
            "TTL < " + frontendConfig.maxAcceptableTtlSecsIfTtlRequired + " will be required for future uploads");
      }
    }
    // inject encryption frontendMetrics if applicable
    if (blobProperties.isEncrypted()) {
      restRequest.getMetricsTracker()
          .injectMetrics(frontendMetrics.postBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), true));
    }
    byte[] userMetadata = RestUtils.buildUserMetadata(restRequest.getArgs());
    frontendMetrics.blobPropsBuildTimeInMs.update(System.currentTimeMillis() - propsBuildStartTime);
    LOGGER.trace("Blob properties of blob being POSTed - {}", blobProperties);
    return new BlobInfo(blobProperties, userMetadata);
  }

  private PutBlobOptions getPutBlobOptionsFromRequest(RestRequest restRequest) throws RestServiceException {
    PutBlobOptionsBuilder builder =
        new PutBlobOptionsBuilder().chunkUpload(RestUtils.isChunkUpload(restRequest.getArgs()))
            .restRequest(restRequest);
    Long maxUploadSize = RestUtils.getLongHeader(restRequest.getArgs(), RestUtils.Headers.MAX_UPLOAD_SIZE, false);
    if (maxUploadSize != null) {
      builder.maxUploadSize(maxUploadSize);
    }
    return builder.build();
  }

  private void setSignedIdMetadataAndBlobSize(RestRequest restRequest, RestResponseChannel restResponseChannel,
      BlobProperties blobProperties) throws RestServiceException {
    if (RestUtils.isChunkUpload(restRequest.getArgs())) {
      Map<String, String> metadata = new HashMap<>(2);
      metadata.put(RestUtils.Headers.BLOB_SIZE, Long.toString(restRequest.getBlobBytesReceived()));
      metadata.put(RestUtils.Headers.SESSION,
          RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.SESSION, true));
      metadata.put(EXPIRATION_TIME_MS_KEY,
          Long.toString(Utils.addSecondsToEpochTime(time.milliseconds(), blobProperties.getTimeToLiveInSeconds())));
      restRequest.setArg(RestUtils.InternalKeys.SIGNED_ID_METADATA_KEY, metadata);
    }
    //the actual blob size is the number of bytes read
    restResponseChannel.setHeader(RestUtils.Headers.BLOB_SIZE, restRequest.getBlobBytesReceived());
  }

  List<ChunkInfo> getChunksToStitch(RestResponseChannel restResponseChannel, BlobProperties stitchedBlobProperties,
      JSONObject stitchRequestJson) throws RestServiceException {
    List<String> signedChunkIds = StitchRequestSerDe.fromJson(stitchRequestJson);
    if (signedChunkIds.isEmpty()) {
      throw new RestServiceException("Must provide at least one ID in stitch request",
          RestServiceErrorCode.MissingArgs);
    }
    List<ChunkInfo> chunksToStitch = new ArrayList<>(signedChunkIds.size());
    String expectedSession = null;
    long totalStitchedBlobSize = 0;
    for (String signedChunkId : signedChunkIds) {
      signedChunkId =
          RequestPath.parse(signedChunkId, Collections.emptyMap(), frontendConfig.pathPrefixesToRemove, clusterName)
              .getOperationOrBlobId(false);
      if (!idSigningService.isIdSigned(signedChunkId)) {
        throw new RestServiceException("All chunks IDs must be signed: " + signedChunkId,
            RestServiceErrorCode.BadRequest);
      }
      Pair<String, Map<String, String>> idAndMetadata = idSigningService.parseSignedId(signedChunkId);
      String blobId = idAndMetadata.getFirst();
      Map<String, String> metadata = idAndMetadata.getSecond();

      expectedSession = RestUtils.verifyChunkUploadSession(metadata, expectedSession);
      @SuppressWarnings("ConstantConditions")
      long chunkSizeBytes = RestUtils.getLongHeader(metadata, RestUtils.Headers.BLOB_SIZE, true);

      totalStitchedBlobSize += chunkSizeBytes;
      // Expiration time is sent to the router, but not verified in this handler. The router is responsible for making
      // checks related to internal ambry requirements, like making sure that the chunks do not expire before the
      // metadata blob.
      @SuppressWarnings("ConstantConditions")
      long expirationTimeMs = RestUtils.getLongHeader(metadata, EXPIRATION_TIME_MS_KEY, true);
      verifyChunkAccountAndContainer(blobId, stitchedBlobProperties);

      chunksToStitch.add(new ChunkInfo(blobId, chunkSizeBytes, expirationTimeMs));
    }
    //the actual blob size for stitched blob is the sum of all the chunk sizes
    restResponseChannel.setHeader(RestUtils.Headers.BLOB_SIZE, totalStitchedBlobSize);
    return chunksToStitch;
  }

  private void verifyChunkAccountAndContainer(String chunkBlobId, BlobProperties stitchedBlobProperties)
      throws RestServiceException {
    Pair<Short, Short> accountAndContainer;
    try {
      accountAndContainer = BlobId.getAccountAndContainerIds(chunkBlobId);
    } catch (Exception e) {
      throw new RestServiceException("Invalid blob ID in signed chunk ID", RestServiceErrorCode.BadRequest);
    }
    if (stitchedBlobProperties.getAccountId() != accountAndContainer.getFirst()
        || stitchedBlobProperties.getContainerId() != accountAndContainer.getSecond()) {
      throw new RestServiceException(
          "Account and container for chunk: (" + accountAndContainer.getFirst() + ", " + accountAndContainer.getSecond()
              + ") does not match account and container for stitched blob: (" + stitchedBlobProperties.getAccountId()
              + ", " + stitchedBlobProperties.getContainerId() + ")", RestServiceErrorCode.BadRequest);
    }
  }
}
