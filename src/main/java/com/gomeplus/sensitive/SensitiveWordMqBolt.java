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
public class SensitiveWordMqBolt extends BaseRichBolt {

    private Logger loggers =  LoggerFactory.getLogger(SensitiveWordMqBolt.class);

    private OutputCollector collector;

    private static WordFilter wordFilter = null;

    private static final String  IS_SENSITIVE = "isSensitive";

    public static synchronized WordFilter getWordFilter(){
        return  wordFilter ==null ? (wordFilter = new WordFilter()): wordFilter;
    }

    private String jsonText = null;

    //设置需要解析的json格式
    public int setJsonText(String jsonTexts,int num){
        this.jsonText = jsonTexts;
        return num * 2;
    }

    public void execute(Tuple tuple) {
        if(tuple.size()>0){
            String text = tuple.getString(0);
            getWordFilter();
            wordFilter.setJsonText(jsonText);
            String content = wordFilter.getText(text);
            if(!content.isEmpty() ) {
                collector.emit(tuple, new Values(content));
            }
                /*
                loggers.info(content);
                boolean textIsSensitive = wordFilter.semanticAnalysis(content);
                // 如果这句话不含有敏感词汇
                if(!textIsSensitive){
                    jsonObject.put(IS_SENSITIVE, false);
                }else{
                    jsonObject.put(IS_SENSITIVE, true);
                }
            }else{
                jsonObject.put(IS_SENSITIVE, false);
            }
            //String resultText = jsonObject.toString();
            collector.emit(tuple,new Values(resultText));*/
            }
        collector.ack(tuple);
    }

    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text"));
    }
}
