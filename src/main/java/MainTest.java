import com.gomeplus.sensitive.WordFilter;
import org.elasticsearch.index.analysis.IkAnalyzerProvider;
import org.wltea.analyzer.dic.Dictionary;

import java.util.ArrayList;

/**
 * Created by wangxiaojing on 2016/9/22.
 */
public class MainTest {
    public static void main(String [] args) throws Exception{
        String url ="http://10.69.10.16:9200/_analyze";
        String para = "text=这几年数据挖掘的概念实在是太耳熟能详";
        // String  str = HttpSendGet(url,para);
        //System.out.println(str);
        //filter("str");


        WordFilter wordFilterMy = new WordFilter();
        //boolean result = wordFilterMy.deleteEsIndex("gome");
        //System.out.println(result);
        //ArrayList<String> words = new ArrayList<String>();
        //words.add("出售冰毒");
        //words.add("出售炸弹");

        //Dictionary.getSingleton().addStopWords(words);
        wordFilterMy.semanticAnalysis("眼角膜亚硝酰乙氧亚硒酸二钠求肾，妈妈不喜欢吃肉,出售手枪出售炸弹出售冰毒");
        //wordFilterMy.deleteEs();
        //String word = new String("0106658.cn".getBytes(),"UTF-8");
        //wordFilterMy.searchWord(word);
        // wordFilterMy.fileCreateIndex();
        wordFilterMy.createIndex("彩宝我试试");
        wordFilterMy.searchWord("彩宝");
        boolean result = wordFilterMy.deleteEs("彩宝我试试");
        System.out.println(result);
        //wordFilterMy.searchWord("woe");
        //wordFilterMy.createIndex("woe2");
        //wordFilterMy.createIndex("3344");
       // wordFilterMy.getIndex();
    }
}
