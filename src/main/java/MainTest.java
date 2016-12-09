import com.gomeplus.sensitive.WordFilter;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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
        //wordFilterMy.semanticAnalysis("眼角膜亚硝酰乙氧亚硒酸二钠求肾，妈妈不喜欢吃肉,出售手枪出售炸弹出售冰毒");
        wordFilterMy.fileCreateIndex();
        int i = wordFilterMy.createIndex("彩宝我试试");
        System.out.println("return is " + i);
        ConcurrentHashMap<String,String> searchHits = wordFilterMy.searchAllWord("彩宝我试试");
        System.out.println(searchHits.toString());
        //boolean result = wordFilterMy.deleteEs("彩宝我试试");
        //System.out.println(result);

        //boolean result1 = wordFilterMy.deleteEsWordId("AViEy4wicawEBYnALN9Q");
        //System.out.println(result1);

        for (String arg : args) {
            int re = wordFilterMy.createIndex(arg);
            System.out.println("add indexReturn is " + re);
        }
    }
}
