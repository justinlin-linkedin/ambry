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
package com.github.ambry.frontend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.server.StatsReportType;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.util.GregorianCalendar;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.commons.FutureUtils.*;
import static com.github.ambry.frontend.FrontendUtils.*;


class GetStatsReportHandler {
  private static final Logger logger = LoggerFactory.getLogger(GetStatsReportHandler.class);

  private final ObjectMapper mapper = new ObjectMapper();
  private final SecurityService securityService;
  private final FrontendMetrics metrics;
  private final AccountStatsStore accountStatsStore;

  /**
   * Constructs a handler for handling requests for getting stats reports.
   * @param securityService the {@link SecurityService} to use.
   * @param metrics the {@link FrontendMetrics} to use.
   * @param accountStatsStore the {@link AccountStatsStore} to use.
   */
  GetStatsReportHandler(SecurityService securityService, FrontendMetrics metrics, AccountStatsStore accountStatsStore) {
    this.securityService = securityService;
    this.metrics = metrics;
    this.accountStatsStore = accountStatsStore;
  }

  /**
   * Asynchronously get the stats reports.
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready or if there is a exception.
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    RestRequestMetrics requestMetrics =
        metrics.getStatsReportMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
    restRequest.getMetricsTracker().injectMetrics(requestMetrics);
    String context = restRequest.getUri();
    CompletableFuture.completedFuture(null)
        .thenCompose(v -> buildContextOnFuture(() -> securityService.processRequest(restRequest),
            metrics.getStatsReportSecurityProcessRequestMetrics, context, logger))
        .thenCompose(v -> buildContextOnFuture(() -> securityService.postProcessRequest(restRequest),
            metrics.getStatsReportSecurityPostProcessRequestMetrics, context, logger))
        .thenCompose(v -> fromCallable(() -> fetchStatsReport(restRequest, restResponseChannel)))
        .whenComplete((channel, exception) -> {
          if (exception != null) {
            callback.onCompletion(null, Utils.extractFutureExceptionCause(exception));
          } else {
            callback.onCompletion(channel, null);
          }
        });
  }

  private ReadableStreamChannel fetchStatsReport(RestRequest restRequest, RestResponseChannel channel)
      throws RestServiceException {
    String clusterName = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.CLUSTER_NAME, true);
    String reportType = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.GET_STATS_REPORT_TYPE, true);
    StatsReportType statsReportType;
    try {
      statsReportType = StatsReportType.valueOf(reportType);
    } catch (Exception e) {
      throw new RestServiceException(reportType + " is not a valid StatsReportType", RestServiceErrorCode.BadRequest);
    }
    Object result;
    try {
      switch (statsReportType) {
        case ACCOUNT_REPORT:
          result = accountStatsStore.queryAggregatedAccountStorageStatsByClusterName(clusterName);
          break;
        case PARTITION_CLASS_REPORT:
          result = accountStatsStore.queryAggregatedPartitionClassStorageStatsByClusterName(clusterName);
          break;
        default:
          throw new RestServiceException("StatsReportType " + statsReportType + "not supported",
              RestServiceErrorCode.BadRequest);
      }
    } catch (Exception e) {
      if (e instanceof RestServiceException) {
        throw (RestServiceException) e;
      }
      throw new RestServiceException("Couldn't query snapshot", e, RestServiceErrorCode.InternalServerError);
    }
    if (result == null) {
      throw new RestServiceException("StatsReport not found for clusterName " + clusterName,
          RestServiceErrorCode.NotFound);
    }
    try {
      byte[] jsonBytes = mapper.writeValueAsBytes(result);
      channel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      channel.setHeader(RestUtils.Headers.CONTENT_TYPE, RestUtils.JSON_CONTENT_TYPE);
      channel.setHeader(RestUtils.Headers.CONTENT_LENGTH, jsonBytes.length);
      return new ByteBufferReadableStreamChannel(ByteBuffer.wrap(jsonBytes));
    } catch (Exception e) {
      throw new RestServiceException("Couldn't serialize snapshot", e, RestServiceErrorCode.InternalServerError);
    }
  }
}
