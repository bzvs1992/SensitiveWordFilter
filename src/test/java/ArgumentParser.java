import com.gomeplus.util.Conf;

/**
 * Created by wangxiaojing on 2016/12/6.
 */
public class ArgumentParser {

    public static void main(String[] args) throws Exception{
      /* Options opts = new Options();
       Option property = OptionBuilder.withArgName("actor=value")
               .hasArgs(2).withValueSeparator().withDescription(
                       "set box actor  master or slave ").create("D");
       property.setRequired(true);
       opts.addOption(property);
       CommandLineParser parser = new PosixParser();
       CommandLine cl = parser.parse(opts, args);
       if(cl.hasOption("D")){
           String actor =  cl.getOptionProperties("D").getProperty("actor");
           String d = cl.getOptionProperties("D").getProperty("word","word");
           System.out.println(actor);
           System.out.println(d);
       }else{
           System.out.println("wwww");
       }
       */
        Conf argumentParser = new Conf();
        argumentParser.parse(args);
        System.out.print(argumentParser.getStormToKafkaTopic());
    }
}
