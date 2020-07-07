package cn.ljq.web;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class WebLogParser extends Mapper<LongWritable, Text,WebLogBean, NullWritable> {
    public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

    Set<String> pages = new HashSet<String>();
    public static String formatDate(String time_local) {
        try {
            return df2.format(df1.parse(time_local));
        } catch (Exception e) {
            return null;
        }

    }
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        WebLogBean webLogBean = new WebLogBean();
        String[] split = value.toString().split(" ");
        if (split.length>11){
            webLogBean.setRemote_addr(split[0]);
            webLogBean.setRemote_user(split[1]);
            String time_local = formatDate(split[3].substring(1));
            if (time_local==null||time_local.equals("")) time_local="-invalid_time-";
            webLogBean.setTime_local(time_local);
            webLogBean.setRequest(split[6]);
            webLogBean.setStatus(split[8]);
            webLogBean.setBody_bytes_sent(split[9]);
            webLogBean.setHttp_referer(split[10]);
            if (split.length>12){
                StringBuilder sb = new StringBuilder();
                for (int i=11;i<split.length;i++){
                    sb.append(split[i]);
                }
                webLogBean.setHttp_user_agent((sb.toString()));
            }else {
                webLogBean.setHttp_user_agent(split[11]);
            }
            if (Integer.parseInt(webLogBean.getStatus())>=400){
                webLogBean.setValid(false);
            }
            if("-invalid_time-".equals(webLogBean.getTime_local())){
                webLogBean.setValid(false);
            }
            if (!pages.contains(webLogBean.getRequest())) {
                webLogBean.setValid(false);
            }
        }else {
            webLogBean=null;
        }
        context.write(webLogBean,NullWritable.get());
    }
}
