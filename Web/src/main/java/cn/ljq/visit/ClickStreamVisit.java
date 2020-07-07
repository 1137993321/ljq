package cn.ljq.visit;

import cn.ljq.pageviews.PageViewsBean;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class ClickStreamVisit {
    static class ClickStreamVisitMapper extends Mapper<LongWritable, Text,Text, PageViewsBean>{
        PageViewsBean pvBean = new PageViewsBean();
        Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\001");
            int step = Integer.parseInt(fields[5]);
            pvBean.set(fields[0], fields[1], fields[3], fields[8], step,fields[4], fields[10], fields[7], fields[6], fields[2]);
            k.set(fields[0]);
            context.write(k,pvBean);
        }
    }
    static class ClickStreamVisitReducer extends Reducer<Text,PageViewsBean, NullWritable,VisitBean>{
        @Override
        protected void reduce(Text key, Iterable<PageViewsBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<PageViewsBean> pvBeansList = new ArrayList<>();
            for (PageViewsBean pvBean : values){
                PageViewsBean bean = new PageViewsBean();
                try {
                    BeanUtils.copyProperties(bean,pvBean);
                    pvBeansList.add(bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Collections.sort(pvBeansList, new Comparator<PageViewsBean>() {
                @Override
                public int compare(PageViewsBean o1, PageViewsBean o2) {
                    return o1.getStep() > o2.getStep() ? 1:-1;
                }
            });
            VisitBean visitBean = new VisitBean();
            visitBean.setInPage(pvBeansList.get(0).getRequest());
            visitBean.setInTime(pvBeansList.get(0).getTimestr());
            visitBean.setOutPage(pvBeansList.get(pvBeansList.size()-1).getRequest());
            visitBean.setOutTime(pvBeansList.get(pvBeansList.size()-1).getTimestr());
            visitBean.setPageVisits(pvBeansList.size());
            visitBean.setRemote_addr(pvBeansList.get(0).getRemote_addr());
            visitBean.setReferal(pvBeansList.get(0).getReferal());
            visitBean.setSession(String.valueOf(key));
            context.write(NullWritable.get(),visitBean);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration cof = new Configuration();
        Job job = Job.getInstance(cof);
        job.setJarByClass(ClickStreamVisit.class);
        job.setMapperClass(ClickStreamVisitMapper.class);
        job.setReducerClass(ClickStreamVisitReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageViewsBean.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(VisitBean.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean s = job.waitForCompletion(true);
        System.exit(s?0:1);
    }
}
