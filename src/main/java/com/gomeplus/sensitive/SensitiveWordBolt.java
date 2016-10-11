package com.gomeplus.sensitive;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by wangxiaojing on 2016/9/29.
 */
public class SensitiveWordBolt extends BaseRichBolt {

    private Logger loggers =  LoggerFactory.getLogger(SensitiveWordBolt.class);

    private OutputCollector collector;

    private  static WordFilter wordFilter = null;


    public static synchronized WordFilter getWordFilter(){
        return  wordFilter ==null ? (wordFilter = new WordFilter()): wordFilter;
    }
    public void execute(Tuple tuple) {
        String text = tuple.getString(0);
        getWordFilter();
        boolean textIsSensitive = wordFilter.semanticAnalysis(text);
        // 如果这句话不含有敏感词汇
        if(!textIsSensitive){
            collector.emit(tuple,new Values(text));
        }

    }

    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text"));
    }
}
