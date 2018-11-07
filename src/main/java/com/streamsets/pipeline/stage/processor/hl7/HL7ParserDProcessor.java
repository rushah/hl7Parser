/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.hl7;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.configurablestage.DProcessor;

@StageDef(
    version=1,
    label="HL7 Parser",
    description = "Parses a string field which contains an HL7 message",
    icon= "hl7.png",
    //TODO -> Fix this url
    onlineHelpRefUrl = "index.html#Processors/LogParser.html#task_jm1_b4w_fs"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class HL7ParserDProcessor extends DProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "/text",
      label = "Field to Parse",
      description = "String field that contains the HL7 message",
      displayPosition = 10,
      group = "HL7_V2"
  )
  @FieldSelectorModel(singleValued = true)
  public String fieldPathToParse;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "/hl7",
      label = "New Parsed Field",
      description="Name of the new field to set the parsed HL7 message",
      displayPosition = 20,
      group = "HL7_V2"
  )
  public String parsedFieldPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "AUTO",
      label = "HL7 Version",
      description = "HL7 Version",
      displayPosition = 30,
      group = "HL7_V2"
  )
  @ValueChooserModel(HL7VersionChooserValues.class)
  public HL7Version hl7Version = HL7Version.AUTO;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Validate Message",
      description = "Validates incoming HL7 message",
      displayPosition = 40,
      group = "HL7_V2"
  )
  public boolean validateMessage;

  @Override
    protected Processor createProcessor() { return new HL7ParserProcessor(fieldPathToParse, parsedFieldPath, hl7Version, validateMessage); }
}
