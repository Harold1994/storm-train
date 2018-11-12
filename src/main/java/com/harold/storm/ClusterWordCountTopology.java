package com.harold.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ClusterWordCountTopology {
    public static class DataSourceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        public void nextTuple() {
            try {
                Collection<File> files = FileUtils.listFiles(new File("/Users/harold/Documents/Code/storm-train/src/main/resources/wc"), new String[]{"txt"}, true);
                for (File file:files) {
                    List<String> lines =  FileUtils.readLines(file, "UTF-8");
                    for (String line : lines) {
                        this.collector.emit(new Values(line));
                    }
                    FileUtils.moveFile(file ,new File(file.getAbsolutePath() + System.currentTimeMillis()));
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    public static class SplitBolt extends BaseRichBolt{
        private OutputCollector collector;
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        public void execute(Tuple input) {
            String line = input.getStringByField("line");
            for(String word : line.split(" ")) {
                this.collector.emit(new Values(word));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class CountBolt extends BaseRichBolt{
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }
        Map<String,Integer> map = new HashMap<String, Integer>();
        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if (null == count)
                map.put(word,1);
            else
                map.put(word, count+1);
            System.out.println("～～～～～～～～～～～～～～");
            Set<Map.Entry<String, Integer>> entries = map.entrySet();
            for (Map.Entry<String, Integer> entry : entries) {
                System.out.println(entry);
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) throws IOException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout(),2);
        builder.setBolt("SplitBolt", new SplitBolt(),1).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt", new CountBolt(),1).shuffleGrouping("SplitBolt").setNumTasks(2);

        Config config = new Config();
        config.setNumWorkers(2);
        config.setNumAckers(1);
        StormSubmitter submitter = new StormSubmitter();
        try {
            submitter.submitTopology(ClusterWordCountTopology.class.getSimpleName(),config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
