package com.gomeplus.sensitive;

/**
 * Created by wangxiaojing on 2016/9/19.
 */


import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.*;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.*;
import java.net.*;
import java.util.List;

import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gomeplus.util.Conf;


public class WordFilter {

    private final static String WORD = "word";

    private final static String GOME = "gome";

    private final static String DATA_DIR = "data\\word";

    private final static String CHARSET = "UTF-8";

    private final static int ES_PORT = 9300;

    // 自定义词典库文件
    private final static String EXT_DICT = "mydict.dic";

    //创建Es客户端
    private static TransportClient client;

    private Logger loggers;

    private Conf conf;

    /**
     * 构造函数，负责读取配置文件，完成Es设置
     */
    public WordFilter() {
        conf = new Conf();
        loggers = LoggerFactory.getLogger(WordFilter.class);
        String[] esHostname = conf.getEsHostname().split(",");
        String clusterName = conf.getEsClusterName();
        InetSocketAddress inetSocketAddress = null;
        for (String hostname : esHostname) {
            inetSocketAddress = new InetSocketAddress(hostname, ES_PORT);
        }
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clusterName).build();
        client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(inetSocketAddress));
    }

    /**
     * 创建Es索引,id自动添加
     *
     * @param str 敏感词
     */
    public void createIndex(String str) {
        if (!str.isEmpty()) {
            //创建数据内容
            try {
                String word = new String(str.getBytes("UTF-8"), CHARSET);
                XContentBuilder builder = jsonBuilder()
                        .startObject()
                        .field("word", word)
                        .endObject();
                IndexResponse response = client.prepareIndex(GOME, WORD).setSource(builder).get();
                loggers.info(response.getId() + "   index: " + response.getIndex() + " word:  " + word);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 通过指定目录创建es索引
     * 需要文件格式是 UTF-8 编码
     * 所有文件存储在 data目录下
     */
    public void fileCreateIndex() {
        File dataDir = new File(DATA_DIR);
        if (dataDir.exists() & dataDir.isDirectory()) {
            // 获取目录下文件列表
            String[] children = dataDir.list();
            for (String fileName : children) {
                File dataFile = new File(DATA_DIR, fileName);
                if (dataFile.exists() && dataFile.isFile()) {
                    try {
                        FileInputStream fis = new FileInputStream(dataFile);
                        InputStreamReader isr = new InputStreamReader(fis, CHARSET);
                        BufferedReader reader = new BufferedReader(isr);
                        String tempString = null;
                        // 按行读取文件内容
                        while ((tempString = reader.readLine()) != null) {
                            String word = tempString.trim();
                            //一行创建一个敏感词的的索引,如果敏感词库中已经包含该词，则不再继续创建索引
                            boolean exitSensitiveWord = searchWord(word);
                            if (!exitSensitiveWord) {
                                createIndex(word);
                            }
                        }
                        isr.close();
                        fis.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            System.out.println();
        }
    }

    /**
     * 创建搜索ES关键词
     *
     * @param str 被搜索的词
     * @return 如果存在敏感词，返回true，否则返回false
     */
    public boolean searchWord(String str) {
        //如果查询字符不为空
        boolean result = false;
        if (str != null & !str.isEmpty()) {
            try {
                // 直接使用termQuery 无法查询中文
                //QueryBuilders.termQuery("word", str.trim());
                QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("word", str.trim());
                SearchResponse response = client.prepareSearch(GOME).setTypes(WORD)
                        .setQuery(queryBuilder).execute().actionGet();
                SearchHits hits = response.getHits();
                //如果搜索到关键词，那么就意味着这个词是敏感词
                if (hits.totalHits() > 0) {
                    for (SearchHit hit : hits) {
                        // 如果查找到立刻返回，不在做过多的判断
                        if (hit.getSource().containsValue(str)) {
                            loggers.info("Sensitive word  : " + str);
                            result = true;
                            return result;
                        }
                    }
                }
            } catch (IndexNotFoundException e) {
                e.printStackTrace();
            }

        }
        return result;
    }

    /**
     * 对输入文档进行中文分词操作，递归查询每个分词是否是敏感词，
     *
     * @return : 当存在敏感词语句是返回true，否则返回false
     */
    public boolean semanticAnalysis(String text) {
        boolean result = false;
        if (text != null & !text.isEmpty()) {
            AnalyzeResponse analyzeResponse = client.admin().indices().prepareAnalyze(text)
                    .setAnalyzer("ik_smart").execute().actionGet();
            List<AnalyzeToken> list = analyzeResponse.getTokens();
            if (list.isEmpty()) {
                return true;
            } else {
                for (AnalyzeToken analyzeToken : list) {
                    String word = analyzeToken.getTerm();
                    //如果是敏感词
                    boolean isSensitive = searchWord(word);
                    if (isSensitive) {
                        int startOffset = analyzeToken.getStartOffset();
                        int endOffset = analyzeToken.getEndOffset();
                        //递归查询是否是敏感词
                        for (int forward = 0; forward >= -2; forward--) {
                            for (int backward = 1; backward <= 2; backward++) {
                                int newStartOffset = (startOffset + forward) < 0 ? 0 : startOffset + forward;
                                int newEndOffset = (endOffset + backward) > text.length() ? text.length() : endOffset + backward;
                                String newWord = text.substring(newStartOffset, newEndOffset);
                                boolean newWordIsSensitive = searchWord(newWord);
                                // 是敏感词则返回true
                                if (newWordIsSensitive) {
                                    result = true;
                                    return result;
                                }
                            }
                        }
                        loggers.info("analyze Sensitive word : " + word);
                        result = true;
                        return result;
                    }
                    loggers.info(" term :" + analyzeToken.getTerm() + "\t position : " + analyzeToken.getPosition());
                }
                return result;
            }
        } else {
            return result;
        }
    }

    /**
     * 获取某个词的索引职位，目前不能使用
     */
    public void getIndex() {
        GetResponse getRequestBuilder = client.prepareGet().get();
        loggers.info("source :" + getRequestBuilder.getSource());

    }

    /**
     * 删除指定数据，目前测试不成功
     */
    public DeleteRequestBuilder deleteEs(String id) {
        client.admin().indices().prepareDelete(id);
        return client.prepareDelete(GOME, WORD, id);
    }
}
