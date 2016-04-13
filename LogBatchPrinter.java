package com.xiaomi.infra.kafka.storm;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class LogBatchPrinter implements Aggregator<ArrayList<String>> {
	
	Log LOG = LogFactory.getLog(LogBatchPrinter.class);
	
	// set topology hdfs file path
	private String topologyFilePath = "hdfs://yongxing/test";
	private String fileOfDate;
	private Configuration conf;
	private String hdfsFile;
	private String hdfsFileTmp;
	private FileSystem fileSystem;
	private FSDataOutputStream output;
	private String batchId;
	private int partitionId;
	
	private String topologyInstanceId = UUID.randomUUID().toString();
	
  @Override
  public void aggregate(ArrayList<String> logList, TridentTuple tuple,
      TridentCollector collector) {
    // aggregate all the log records together in the batch
    String log = tuple.toString();
    logList.add(log);
    //LOG.info("The size of logList is: " + logList.size());
    //LOG.info("The address of logList is: " + System.identityHashCode(logList));
    
    // write to hdfs
    try {
      this.output.write(log.getBytes());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void complete(ArrayList<String> logList, TridentCollector collector) {
    LOG.info("The topology UUID: " + this.topologyInstanceId);
    //LOG.info("The size of partition is: " + logList.size());
    //LOG.info("The address of logList in complete: " + System.identityHashCode(logList));
    
    if (logList.size() == 0) {
      LOG.warn("The logList is null for batchId: " + this.batchId
          + " And delete the tmp file: " + this.hdfsFileTmp);
      
      try {
        this.fileSystem.delete(new Path(this.hdfsFileTmp), true);
      } catch (IOException e) {
        e.printStackTrace();
      }
      
      return;
    }
    
    // rename and close fileSystem
    try {
      this.output.flush();      
      this.output.close();
      // if the renamed file exists, delete it
      if (this.fileSystem.exists(new Path(this.hdfsFile))) {
        this.fileSystem.delete(new Path(this.hdfsFile), true);
      }
      
      this.fileSystem.rename(new Path(this.hdfsFileTmp), new Path(this.hdfsFile));
      
      //this.fileSystem.close();
      
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    // clear data
    logList.clear();
  }

  @Override
  public ArrayList<String> init(Object batchId, TridentCollector collector) {

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM-dd-yyyy");
    this.fileOfDate = simpleDateFormat.format(new Date());
    this.conf = new HdfsConfiguration();
    this.batchId = batchId.toString().split(":")[0];
    
    this.hdfsFile = this.topologyFilePath + "/" + this.fileOfDate + "/"
        + this.topologyInstanceId + "-" + this.batchId 
        + "_" + this.partitionId + ".batchlog";
    
    this.hdfsFileTmp = this.hdfsFile + ".tmp";
    
    LOG.info("Processing batch for batchId: " + batchId.toString());
    
    try {
      this.fileSystem = FileSystem.get(this.conf);
      
      // if the tmp file exists, delete it
      if (this.fileSystem.exists(new Path(this.hdfsFileTmp))) {
        this.fileSystem.delete(new Path(this.hdfsFileTmp), true);
      }
      
      // create a new file
      this.output = this.fileSystem.create(
          new Path(this.hdfsFileTmp), new Progressable() {
            public void progress() { }
          });
      
      LOG.info("Create hdfs temp file: " + this.hdfsFileTmp);
          
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    return new ArrayList<String>();
    //ArrayList<String> tempArrayList = new ArrayList<String>();
    //LOG.info("The tempArrayList address is: " + System.identityHashCode(tempArrayList));
    //return tempArrayList;
  }

  @Override
  public void cleanup() {
    
  }

  @Override
  public void prepare(Map map, TridentOperationContext tridentOperationContext) {
    // set kerberos authentication
    System.setProperty("java.security.krb5.conf", 
        System.getProperty("user.dir") + "/krb5.conf");
    
    this.partitionId = tridentOperationContext.getPartitionIndex();
  }

}
