package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceFactory;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;


public class AccountServiceModule extends AbstractModule {
  @Provides
  public AccountService provideAccountService(ServerConfig serverConfig, VerifiableProperties verifiableProperties,
      MetricRegistry registry) {
    return ((AccountServiceFactory) Utils.getObj(serverConfig.serverAccountServiceFactory, verifiableProperties,
        registry)).getAccountService();
  }
}
