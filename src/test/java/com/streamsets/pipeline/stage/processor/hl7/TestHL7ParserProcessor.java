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
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.DefaultXMLParser;
import ca.uhn.hl7v2.parser.XMLParser;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHL7ParserProcessor {

  String v25message = "MSH|^~\\&|ULTRA|TML|OLIS|OLIS|200905011130||ORU^R01|20169838-v25|T|2.5\r"
      + "PID|||7005728^^^TML^MR||TEST^RACHEL^DIAMOND||19310313|F|||200 ANYWHERE ST^^TORONTO^ON^M6G 2T9||(416)888-8888||||||1014071185^KR\r"
      + "PV1|1||OLIS||||OLIST^BLAKE^DONALD^THOR^^^^^921379^^^^OLIST\r"
      + "ORC|RE||T09-100442-RET-0^^OLIS_Site_ID^ISO|||||||||OLIST^BLAKE^DONALD^THOR^^^^L^921379\r"
      + "OBR|0||T09-100442-RET-0^^OLIS_Site_ID^ISO|RET^RETICULOCYTE COUNT^HL79901 literal|||200905011106|||||||200905011106||OLIST^BLAKE^DONALD^THOR^^^^L^921379||7870279|7870279|T09-100442|MOHLTC|200905011130||B7|F||1^^^200905011106^^R\r"
      + "OBX|1|ST|||Test Value";

  private Record createRecordWithLine(String textString) {
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("text", Field.create(textString));
    record.set(Field.create(map));
    return record;
  }

  @Test
  public void testParsingHL7() throws Exception {

    //Files.write(v25message, new File("/tmp/out/hl7"), Charset.defaultCharset());

    ProcessorRunner runner = new ProcessorRunner.Builder(HL7ParserDProcessor.class)
        .setOnRecordError(OnRecordError.DISCARD)
        .addConfiguration("fieldPathToParse", "/text")
        .addConfiguration("parsedFieldPath", "/parsed")
        .addConfiguration("hl7Version", HL7Version.AUTO)
        .addConfiguration("validateMessage", true)
        .addOutputLane("out")
        .build();
    runner.runInit();
    try {

      Record r0 = createRecordWithLine(v25message);
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);
      Assert.assertNotNull(output.getRecords().get("out").get(0).get("/parsed").getValue());
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
    } finally {
      runner.runDestroy();
    }
  }

}
