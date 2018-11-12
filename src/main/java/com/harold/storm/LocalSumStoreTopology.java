package com.harold.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
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
import org.apache.storm.utils.Utils;

import java.util.Map;

public class LocalSumStoreTopology {
    public static class DataSourceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        /**
         * 初始化
         * @param conf
         * @param topologyContext
         * @param spoutOutputCollector
         */
        public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector = spoutOutputCollector;
        }

        int number = 0;

        public void nextTuple() {
            this.collector.emit(new Values(number ++ ));
            System.out.println("spout: " + number);
            Utils.sleep(1000);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("num"));
        }
    }

    public static class DataBout extends BaseRichBolt {
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }
        int num = 0;
        public void execute(Tuple input) {
            num += input.getIntegerByField("num");
            System.out.println("[total] = " + num);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("DataBout", new DataBout()).shuffleGrouping("DataSourceSpout");
        StormTopology topology = builder.createTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStoreTopology", new Config(), topology);
    }
}
