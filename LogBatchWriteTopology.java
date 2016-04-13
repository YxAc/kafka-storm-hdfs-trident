package com.xiaomi.infra.kafka.storm;

import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.TridentTopology;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

public class LogBatchWriteTopology {
	
	// Kafka broker hosts
	private final BrokerHosts brokerHosts;
	
	// trident topology zk node
	private final String kafkaSpoutId = "logBatchSpout";
	public final String kafkaSpoutZkPath = "/storm/kafka/yx/logBatchPrinterDemo-12";
	
	public LogBatchWriteTopology(String kafkaZookeeper) {
		brokerHosts = new ZkHosts(kafkaZookeeper, "/kafka/yx/brokers");
	}
	
	// Build the storm topology
	public StormTopology buildTopology() {
	  // parameters: TridentKafkaConfig(brokerHosts, topic, clientId)
	  TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(
	      brokerHosts, "access_bucket", "logBatchWriter");
	  kafkaConfig.scheme = new SchemeAsMultiScheme(new KafkaScheme());
		
		// read Kafka from the current time
	  kafkaConfig.forceFromStart = true;
	  kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		
	  // set batch size for message
	  kafkaConfig.fetchSizeBytes = 300 << 20; // about 300M
	  kafkaConfig.bufferSizeBytes = 300 << 20;
	  
	  // set kafka trident spout
	  TransactionalTridentKafkaSpout kafkaSpout = 
	      new TransactionalTridentKafkaSpout(kafkaConfig);
	  
	  // 
	  TridentTopology topology = new TridentTopology();
	  
	  
	  topology.newStream(kafkaSpoutId, kafkaSpout)
	    .parallelismHint(8)  // corresponding to number of kafka partitions
	    //.shuffle()           
	    // the input field "msg" corresponding to the KafkaScheme.java
	    .each(new Fields("msg"), new LogExtract(), new Fields("ip", "time"))
	    .parallelismHint(8)
	    .aggregate(new Fields("msg", "ip", "time"),
	        new LogBatchPrinter(), new Fields("log_record"));
	    //.partitionBy(new Fields("ip"))
	    //.partitionAggregate(new Fields("msg", "ip", "time"),
	    //    new LogBatchPrinter(), new Fields("log_record"))
	    //.parallelismHint(8);
	  
	  return topology.build();
	}
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		
		LogBatchWriteTopology logBatchWriteTopology = new LogBatchWriteTopology(args[0]);
		Config config = new Config();
		
		// set the configuration for topology
		config.put(Config.TOPOLOGY_ACKER_EXECUTORS, 100);
		
		// set trident batch emit interval time for batch size: 50s
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 50000);
		// set trident topology zk node, please configure this item !
		config.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT,
		    logBatchWriteTopology.kafkaSpoutZkPath);
		// set trident batch pending size, no need too large for trident, suggesting less than 10.
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);
		
		StormTopology stormTopology = logBatchWriteTopology.buildTopology();
		
		// Running the topology on a storm cluster
		if (args != null && args.length > 1) {
		  if (args.length < 3) {
		    System.err.println("Usage: ./bin/storm jar "
		        + "${your_topology-jar-with-dependencies.jar}" 
		        + "${package.path.main.class} ${kafka's zookeeper list}" 
		        + "${topology_name} ${nimbus_host_ip} ");
		    System.exit(-1);  
		  }
			String topologyName = args[1];
			String nimbusIp = args[2];
			
			// set the worker process number
			config.setNumWorkers(10);
			config.put(Config.NIMBUS_HOST, nimbusIp);
			StormSubmitter.submitTopology(topologyName, config, stormTopology);			
		}else {
			config.setNumWorkers(12);
			config.setMaxTaskParallelism(4);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafka-storm", config, stormTopology);
		}
		
	}
	
}
