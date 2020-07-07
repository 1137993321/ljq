package cn.ljq.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WebMain {
    public static void main(String[] args) throws Exception {
        Configuration cof = new Configuration();
        Job job = Job.getInstance(cof);
        job.setJarByClass(WebMain.class);
        job.setMapperClass(WebLogParser.class);
        job.setOutputKeyClass(WebLogBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setNumReduceTasks(0);
        boolean re = job.waitForCompletion(true);
        System.exit(re?0:1);
    }
}
