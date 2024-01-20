package com.github.ambry.server;

import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.google.inject.AbstractModule;


public class ConfigurationModule extends AbstractModule {

  private final VerifiableProperties properties;

  ConfigurationModule(VerifiableProperties verifiableProperties) {
    this.properties = verifiableProperties;
  }

  @Override
  protected void configure() {
    NetworkConfig networkConfig = new NetworkConfig(properties);
    StoreConfig storeConfig = new StoreConfig(properties);
    DiskManagerConfig diskManagerConfig = new DiskManagerConfig(properties);
    ServerConfig serverConfig = new ServerConfig(properties);
    ReplicationConfig replicationConfig = new ReplicationConfig(properties);
    ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(properties);
    SSLConfig sslConfig = new SSLConfig(properties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
    StatsManagerConfig statsConfig = new StatsManagerConfig(properties);
    CloudConfig cloudConfig = new CloudConfig(properties);
    // verify the configs
    properties.verify();

    bind(NetworkConfig.class).toInstance(networkConfig);
    bind(StoreConfig.class).toInstance(storeConfig);
    bind(DiskManagerConfig.class).toInstance(diskManagerConfig);
    bind(ServerConfig.class).toInstance(serverConfig);
    bind(ReplicationConfig.class).toInstance(replicationConfig);
    bind(ConnectionPoolConfig.class).toInstance(connectionPoolConfig);
    bind(SSLConfig.class).toInstance(sslConfig);
    bind(ClusterMapConfig.class).toInstance(clusterMapConfig);
    bind(StatsManagerConfig.class).toInstance(statsConfig);
    bind(CloudConfig.class).toInstance(cloudConfig);
  }
}
