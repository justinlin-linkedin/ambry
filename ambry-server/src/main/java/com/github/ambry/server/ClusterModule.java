package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.google.inject.AbstractModule;


public class ClusterModule extends AbstractModule {

  private final ClusterAgentsFactory clusterAgentsFactory;

  ClusterModule(ClusterAgentsFactory clusterAgentsFactory) {
    this.clusterAgentsFactory = clusterAgentsFactory;
  }

  @Override
  protected void configure() {
    ClusterMap clusterMap;
    try {
      clusterMap = clusterAgentsFactory.getClusterMap();
    } catch (Exception e) {
      throw new RuntimeException("");
    }
    bind(ClusterMap.class).toInstance(clusterMap);
    bind(MetricRegistry.class).toInstance(clusterMap.getMetricRegistry());
  }
}
