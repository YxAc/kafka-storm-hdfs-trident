package com.xiaomi.infra.kafka.storm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import backtype.storm.tuple.Values;

public class LogExtract extends BaseFunction {
  
  Log LOG = LogFactory.getLog(LogExtract.class);

  @Override
  public void execute(TridentTuple input, TridentCollector collector) {
    String csvRecord = input.getString(0);
    String [] recordWords = csvRecord.split(",");
    if (recordWords.length > 0) {
      String ip = recordWords[0].trim();
      String time = recordWords[1].trim();
      String content = csvRecord;
      
      if (!ip.isEmpty() && !time.isEmpty()) {
        collector.emit(new Values(ip, time));
        //LOG.info("Record content of ip : " + ip + " time: " + time);
      }
    }	
  }
}
