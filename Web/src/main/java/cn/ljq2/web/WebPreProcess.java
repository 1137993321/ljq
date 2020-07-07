package cn.ljq2.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
public class WebPreProcess {
    static class WebPreProcessMapper extends Mapper<LongWritable, Text,WebBean, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            WebBean webBean = WebParser.maper(string);
            if (webBean!=null){
                context.write(webBean,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WebPreProcess.class);
        job.setMapperClass(WebPreProcessMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(WebBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(WebBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("F:\\wordcount\\access.log.20181101.dat"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\wordcount\\output5"));
        job.setNumReduceTasks(0);
        boolean re = job.waitForCompletion(true);
        System.exit(re?0:1);
    }
}
