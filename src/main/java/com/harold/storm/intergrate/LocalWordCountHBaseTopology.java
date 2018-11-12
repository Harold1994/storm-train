package com.harold.storm.intergrate;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
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

public class LocalWordCountHBaseTopology {
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
            declarer.declare(new Fields("word","count"));
        }
    }

    public static void main(String[] args) throws IOException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("DataSourceSpout");

        Config conf = new Config();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("hbase.rootdir", "hdfs://localhost:9000/hbase");
        map.put("hbase.zookeeper.quorum","127.0.0.1:2181");

        conf.put("hbase.conf", map);

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("word")
                .withColumnFields(new Fields("word"))
                .withCounterFields(new Fields("count"))
                .withColumnFamily("cf");

        HBaseBolt hbase = new HBaseBolt("wc", mapper).withConfigKey("hbase.conf");
        builder.setBolt("HBaseBolt", hbase).shuffleGrouping("CountBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountRedisTopology", conf, builder.createTopology());
    }
}
