package com.harold.storm.intergrate;

import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Map;

public class StormKafkaTopo {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpoutConfig.Builder<String, String> kafkaBuilder = new KafkaSpoutConfig.Builder<String, String>("localhost:9092", "stormtopic");
        kafkaBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST)
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName())
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName())
                .setProp(ConsumerConfig.GROUP_ID_CONFIG,"test");
        KafkaSpoutConfig<String, String> kafkaSpoConfig = kafkaBuilder.build();
        KafkaSpout<String, String> spout = new KafkaSpout(kafkaSpoConfig);
        builder.setSpout("kafka_spout", spout);
        builder.setBolt("LogProcessBolt", new LogProcessBolt()).shuffleGrouping("kafka_spout");

        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.cj.jdbc.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/sparktest");
        hikariConfigMap.put("dataSource.user","hive@localhost");
        hikariConfigMap.put("dataSource.password","Hive_123");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "stat";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);

        builder.setBolt("JdbcInsertBolt", userPersistanceBolt).shuffleGrouping("LogProcessBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("StormKafkaTopo",new Config(), builder.createTopology());
    }
}
