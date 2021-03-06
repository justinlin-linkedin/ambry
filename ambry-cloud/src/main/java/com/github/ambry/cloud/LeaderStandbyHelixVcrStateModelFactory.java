/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.participant.statemachine.StateModel;


/**
 * A factory for creating {@link LeaderStandbyHelixVcrStateModel}
 */
public class LeaderStandbyHelixVcrStateModelFactory extends VcrStateModelFactory {

  /**
   * Constructor for {@link LeaderStandbyHelixVcrStateModelFactory}.
   * @param helixVcrClusterParticipant the helix vcr cluster participant.
   */
  public LeaderStandbyHelixVcrStateModelFactory(HelixVcrClusterParticipant helixVcrClusterParticipant) {
    this.helixVcrClusterParticipant = helixVcrClusterParticipant;
  }

  /**
   * Create and return an instance of {@link LeaderStandbyHelixVcrStateModel}
   * @param resourceName the resource name for which this state model is being created.
   * @param partitionName the partition name for which this state model is being created.
   *
   * @return an instance of {@link LeaderStandbyHelixVcrStateModel}.
   */
  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    return new LeaderStandbyHelixVcrStateModel(helixVcrClusterParticipant);
  }

  @Override
  String getStateModelName() {
    return LeaderStandbySMD.name;
  }
}
