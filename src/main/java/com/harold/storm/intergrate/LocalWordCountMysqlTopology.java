package com.harold.storm.intergrate;

import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class LocalWordCountMysqlTopology {
    public static class DataSourceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }
        private static final String[] words = new String[]{"Apple","Orange","pineapple","banana","watermelon","peach"};
        public void nextTuple() {
             Random random = new Random();
             String word = words[random.nextInt(words.length)];
             this.collector.emit(new Values(word));
            System.out.println("emit " + word);
            Utils.sleep(1000);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class CountBolt extends BaseRichBolt{
        private OutputCollector collector;
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        Map<String,Integer> map = new HashMap<String, Integer>();
        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if (null == count)
                count = 1;
            else
                count = count + 1;
            map.put(word, count);
            this.collector.emit(new Values(word, count));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","word_count"));
        }
    }

    public static class sqlBolt extends BaseRichBolt{
        private ConnectionProvider connectionProvider;
        private JdbcClient jdbcClinet;
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            Map hikariConfigMap = Maps.newHashMap();
            hikariConfigMap.put("dataSourceClassName","com.mysql.cj.jdbc.MysqlDataSource");
            hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/sparktest");
            hikariConfigMap.put("dataSource.user","hive@localhost");
            hikariConfigMap.put("dataSource.password","Hive_123");
            connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
            connectionProvider.prepare();
            jdbcClinet = new JdbcClient(connectionProvider, 20);
        }

        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            int num = input.getIntegerByField("word_count");
            String sql;
            if (num == 1) {
                sql = "insert into wc values ('" + word + "', 1 )";
            } else {
                sql = "update wc set word_count=" + num + " where word='"+word+ "'";
            }
            jdbcClinet.executeSql(sql);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }


    public static void main(String[] args) throws IOException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("sqlBolt", new sqlBolt()).shuffleGrouping("CountBolt");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountMysqlTopology", new Config(), builder.createTopology());
    }
}
