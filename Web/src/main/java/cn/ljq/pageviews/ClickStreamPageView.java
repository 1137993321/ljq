package cn.ljq.pageviews;

import cn.ljq.web.WebLogBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ClickStreamPageView {

    static class ClickStreamMapper extends Mapper<LongWritable, Text,Text, WebLogBean>{
        Text k = new Text();
        WebLogBean v = new WebLogBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\001");
            if (split.length<9) return;
            v.set(split[0].equals("true")?true:false,split[1], split[2], split[3], split[4], split[5], split[6], split[7], split[8]);
            if (v.isValid()){
                k.set(v.getRemote_addr());
                context.write(k,v);
            }
        }
    static class ClickStreamReducer extends Reducer <Text, WebLogBean, NullWritable, Text>{
            Text v = new Text();
            @Override
            protected void reduce(Text key, Iterable<WebLogBean> values, Context context) throws IOException, InterruptedException {
                ArrayList<WebLogBean> beans = new ArrayList<WebLogBean>();
            try {
                for (WebLogBean bean : values) {
                    WebLogBean webLogBean = new WebLogBean();
                    try {
                        BeanUtils.copyProperties(webLogBean, bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    beans.add(webLogBean);
                }
                Collections.sort(beans, new Comparator<WebLogBean>() {
                    @Override
                    public int compare(WebLogBean o1, WebLogBean o2) {
                        try {
                            Date d1 = toDate(o1.getTime_local());
                            Date d2 = toDate(o2.getTime_local());
                            if (d1 == null || d2 == null)
                                return 0;
                            return d1.compareTo(d2);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return 0;
                        }
                    }
                });
                int step = 1;
                String session = UUID.randomUUID().toString();
                for (int i = 0; i < beans.size(); i++) {
                    WebLogBean bean = beans.get(i);
                    if (beans.size() == 1) {
                        v.set(session + "\001" + key.toString() + "\001" + bean.getRemote_user() + "\001" + bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step + "\001" + (60) + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent() + "\001" + bean.getBody_bytes_sent() + "\001" + bean.getStatus());
                        context.write(NullWritable.get(), v);
                        session = UUID.randomUUID().toString();
                        break;
                    }
                    if (i == 0) {
                        continue;
                    }
                    long timeDiff = timeDiff(toDate(bean.getTime_local()), toDate(beans.get(i - 1).getTime_local()));
                    if(timeDiff < 30*60*1000){
                        v.set(session+"\001"+key.toString()+"\001"+beans.get(i - 1).getRemote_user() + "\001" + beans.get(i - 1).getTime_local() + "\001" + beans.get(i - 1).getRequest() + "\001" + step + "\001" + (timeDiff / 1000) + "\001" + beans.get(i - 1).getHttp_referer() + "\001"
                                + beans.get(i - 1).getHttp_user_agent() + "\001" + beans.get(i - 1).getBody_bytes_sent() + "\001" + beans.get(i - 1).getStatus());
                        context.write(NullWritable.get(), v);
                        step++;
                    }else {
                        v.set(session+"\001"+key.toString()+"\001"+beans.get(i - 1).getRemote_user() + "\001" + beans.get(i - 1).getTime_local() + "\001" + beans.get(i - 1).getRequest() + "\001" + (step) + "\001" + (60) + "\001" + beans.get(i - 1).getHttp_referer() + "\001"
                                + beans.get(i - 1).getHttp_user_agent() + "\001" + beans.get(i - 1).getBody_bytes_sent() + "\001" + beans.get(i - 1).getStatus());
                        context.write(NullWritable.get(),v);
                        step = 1;
                        session = UUID.randomUUID().toString();
                    }
                    if (i == beans.size()-1){
                        v.set(session+"\001"+key.toString()+"\001"+bean.getRemote_user() + "\001" + bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step + "\001" + (60) + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent() + "\001" + bean.getBody_bytes_sent() + "\001" + bean.getStatus());
                        context.write(NullWritable.get(), v);
                    }
                }
            } catch (Exception e){
                e.printStackTrace();
            }
            }
            private String toStr(Date date) {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
                return df.format(date);
            }
            private Date toDate(String timeStr) throws ParseException {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
                return df.parse(timeStr);
            }
            private long timeDiff(String time1, String time2) throws ParseException {
                Date d1 = toDate(time1);
                Date d2 = toDate(time2);
                return d1.getTime() - d2.getTime();
            }
            private long timeDiff(Date time1, Date time2) throws ParseException {
                return time1.getTime() - time2.getTime();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration cof = new Configuration();
        Job job = Job.getInstance(cof);
        job.setJarByClass(ClickStreamPageView.class);
        job.setMapperClass(ClickStreamMapper.class);
        job.setReducerClass(ClickStreamMapper.ClickStreamReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WebLogBean.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean sc = job.waitForCompletion(true);
        System.exit(sc?0:1);
    }
}
