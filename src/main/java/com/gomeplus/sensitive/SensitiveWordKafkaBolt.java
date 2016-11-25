package com.gomeplus.sensitive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
public class SensitiveWordKafkaBolt extends BaseRichBolt {

    private Logger loggers =  LoggerFactory.getLogger(SensitiveWordKafkaBolt.class);

    private OutputCollector collector;

    private static WordFilter wordFilter = null;

    private static final String  IS_SENSITIVE = "isSensitive";

    public static synchronized WordFilter getWordFilter(){
        return  wordFilter ==null ? (wordFilter = new WordFilter()): wordFilter;
    }
    public void execute(Tuple tuple) {
        if(tuple.size()>0){
            String text = tuple.getString(0);
            getWordFilter();
            String content = wordFilter.getText(text);
            if(null != content){
                collector.emit(tuple,new Values("word",content));
            }
            /*
            JSONObject jsonObject = JSON.parseObject(new String(text.toString()));
            if(null != content && null != jsonObject ){
                loggers.info(content);
                boolean textIsSensitive = wordFilter.semanticAnalysis(content);
                // 如果这句话不含有敏感词汇
                if(!textIsSensitive){
                    jsonObject.put(IS_SENSITIVE, false);
                }else{
                    jsonObject.put(IS_SENSITIVE, true);
                }
                String resultText = jsonObject.toString();
                collector.emit(tuple,new Values("word",resultText));
            }*/
        }
        this.collector.ack(tuple);
    }

    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "message"));
    }
}
