package com.gomeplus.sensitive;

import org.wltea.analyzer.dic.Dictionary;
/**
 * Created by wangxiaojing on 2016/10/9.
 *
 *  主要用于发现语句中的敏感词
 */
public class InsightSensitiveWord {

    // 发现敏感词
    public String insightWord(){
        return  null;
    }

    // 将敏感词更新到es的ik词库中

    public void httpGetCustomDict(){
        Dictionary.getSingleton().reLoadMainDict();
    }
    // 为新的敏感词创建索引


}
