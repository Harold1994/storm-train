package com.harold.storm.intergrate;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.io.IOException;
import java.util.*;

public class LocalWordCountRedisTopology {
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

    static class WordCountStoreMapper implements RedisStoreMapper {
        private RedisDataTypeDescription description;
        private final String hashKey = "wc";

        public WordCountStoreMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.HASH, hashKey);
        }


        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }


        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("word");
        }

        public String getValueFromTuple(ITuple tuple) {
            return tuple.getIntegerByField("count") + "";
        }
    }

    public static void main(String[] args) throws IOException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("DataSourceSpout");

        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("localhost").setPort(6379).build();
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);
        builder.setBolt("RedisStoreBolt", storeBolt).shuffleGrouping("CountBolt");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountRedisTopology", new Config(), builder.createTopology());
    }
}
