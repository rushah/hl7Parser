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

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.*;
import ca.uhn.hl7v2.parser.*;
import ca.uhn.hl7v2.validation.impl.ValidationContextFactory;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.config.OnParseError;
import java.util.*;

public class HL7ParserProcessor extends SingleLaneRecordProcessor {

  private final String fieldPathToParse;
  private final String parsedFieldPath;
  private final HL7Version hl7Version;
  private final Boolean validateMessage;
  private final OnParseError onParseError;
  private HapiContext hapiContext;
  private PipeParser parser;

  public HL7ParserProcessor(String fieldPathToParse, String parsedFieldPath, HL7Version hl7Version, Boolean validateMessage) {
    this.fieldPathToParse = fieldPathToParse;
    this.parsedFieldPath = parsedFieldPath;
    this.hl7Version = hl7Version;
    this.validateMessage = validateMessage;
    this.onParseError = OnParseError.ERROR;

  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    Field field = record.get(fieldPathToParse);
    if (field == null) {
      throw new OnRecordErrorException(Errors.HL7P_00, fieldPathToParse, record.getHeader().getSourceId());
    } else {
      String value = field.getValueAsString();
      if (value == null) {
        throw new OnRecordErrorException(Errors.HL7P_01, fieldPathToParse, record.getHeader().getSourceId());
      } else {
        hapiContext = new DefaultHapiContext();
        if (!hl7Version.getLabel().equals("Auto")) {
          hapiContext.setModelClassFactory(new CanonicalModelClassFactory(hl7Version.getLabel()));
        }
        if (!validateMessage) {
          hapiContext.setValidationContext(ValidationContextFactory.noValidation());
        }
        try {
          parser=hapiContext.getPipeParser();
          Message message = parser.parse(value);
          final Map<String, Field> fields = getFields(message);
          record.set(parsedFieldPath, Field.create(Field.Type.MAP, fields));
        } catch (HL7Exception e) {
          e.printStackTrace();
        }
      }

      if (!record.has(parsedFieldPath)) {
        throw new OnRecordErrorException(Errors.HL7P_02, parsedFieldPath, record.getHeader().getSourceId());
      }
      batchMaker.addRecord(record);
    }
  }

  private Map<String, Field> getFields(Message message) throws HL7Exception {
    Map<String, Field> map = new HashMap<>();
    String[] names = message.getNames();
    for(int i = 0; i < names.length; i++) {
      String name = names[i];
      // Get the segment name
      Structure structure = message.get(name);
      boolean repeating = message.isRepeating(name);
      Map<String, Field> value = new HashMap<>();
      repeating = handleStructure(structure, repeating, value);
      if (repeating) {
        List<Field> fields = new ArrayList<>();
        fields.add(Field.create(value));
        if (!value.isEmpty()) {
          map.put(name, Field.create(Field.Type.LIST, fields));
        }
      } else {
        if (!value.isEmpty()) {
          map.put(name, Field.create(Field.Type.MAP, value));
        }
      }
    }

    return map;
  }

  private boolean handleStructure(
      Structure structure,
      boolean repeating,
      Map<String, Field> value
  ) throws HL7Exception {
    if (structure instanceof AbstractSegment) {
      AbstractSegment segment = (AbstractSegment) structure;
      int numFields = segment.numFields();
      String[] fieldNames = segment.getNames();
      for (int j = 1; j <= numFields; j++) {
        Type[] types = segment.getField(j);
        for (Type type : types) {
          Field fieldFromType = createFieldFromType(type);
          if (null != fieldFromType) {
            value.put(fieldNames[j - 1], fieldFromType);
          }
        }
      }
    } else if (structure instanceof AbstractGroup) {
      AbstractGroup group = (AbstractGroup) structure;
      String[] groupNames = group.getNames();
      for (int j = 1; j <= groupNames.length; j++) {
        Structure s = group.get(groupNames[j-1]);
        Map<String, Field> groups = new HashMap<>();
        handleStructure(s, false, groups);
        if (!groups.isEmpty()) {
          value.put(groupNames[j - 1], Field.create(Field.Type.MAP, groups));
        }
      }
    }
    return repeating;
  }

  private Field createFieldFromType(Type type) {
    Field f = null;
    if (type instanceof AbstractPrimitive) {
      String value = ((AbstractPrimitive) type).getValue();
      if (null != value) {
        f = Field.create(value);
      }
    } else if (type instanceof AbstractComposite) {

      Type[] components = ((AbstractComposite) type).getComponents();
      Map<String, Field> componentFields = new HashMap<>();
      for (int i = 1; i <= components.length; i++ ) {
        Type component = components[i-1];
        Field fieldFromType = createFieldFromType(component);
        if (null != fieldFromType) {
          componentFields.put(String.valueOf(i),fieldFromType);
        }
      }
      if (!componentFields.isEmpty()) {
        f = Field.create(Field.Type.MAP, componentFields);
      }
    }
    return f;
  }
}
