package com.harold.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class LocalDRPCTopology {
    public static class myBolt extends BaseRichBolt{
        private OutputCollector collector;
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        public void execute(Tuple input) {
            long id = input.getLong(0);
            String name = input.getString(1);

            String result = "add user:" + name;
            this.collector.emit(new Values(id, name));

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id","result"));
        }
    }

    public static void main(String[] args) {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("addUser");
        builder.addBolt(new myBolt());
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("addUser", new Config(), builder.createLocalTopology(drpc));

        String result = drpc.execute("addUser","zhangsahn");
        System.out.println("From client:  " + result);
        drpc.shutdown();
    }
}
