/**
 * Copyright 2016 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.hl7;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum HL7Version implements Label {
  AUTO("Auto"),
  v22("2.2"),
  v23("2.3"),
  v231("2.3.1"),
  v24("2.4"),
  v25("2.5"),
  v251("2.5.1"),
  v26("2.6")
  ;

  private final String label;

  HL7Version(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
