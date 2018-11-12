package com.harold.storm.intergrate;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class LogProcessBolt extends BaseRichBolt {
    OutputCollector collector;
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            String log = input.getStringByField("value");
            String [] splits = log.split("\t");
            String phone = splits[0];
            String [] temp = splits[1].split(",");
            String longitude = temp[0];
            String latitude = temp[1];
            long date = DateUtil.getInstance().getTime(splits[2]);
            collector.emit(new Values(date,Double.parseDouble(longitude),Double.parseDouble(latitude)));
            collector.ack(input);
        } catch (Exception e) {
           collector.fail(input);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time","longitude","latitude"));
    }
}
