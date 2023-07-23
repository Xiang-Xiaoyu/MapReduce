package org.xiaoyu.HarryPotter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
import java.text.DecimalFormat;


public class GraphBuilder {
    public static class GraphBuilderMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //输入的value的格式为:"Role0\t[Role1,x.xx|Role2,x.xx|...|Rolen,x.xx]"
            String cur_line = value.toString();
            String[] Items = cur_line.split("\t");
            String Init_PR = "1.0";
            String values = Init_PR + "|" + Items[1].substring(1, Items[1].length() - 1);
            context.write(new Text(Items[0]), new Text(values));
            //发射Key:Role0 Value:1.0|Role1,x.xx|Role2,x.xx|...|Rolen,x.xx
        }
    }

    public static class GraphBuilderReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text v : values) {
                context.write(key, v);
            }
        }

    }

    public static void main(String[] args)
            throws Exception {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "GraphBuilder");//设置环境参数
            job.setJarByClass(GraphBuilder.class);//设置整个程序的类名
            job.setMapperClass(GraphBuilderMapper.class);//设置Mapper类
            job.setReducerClass(GraphBuilderReducer.class);//设置reducer类
            job.setOutputKeyClass(Text.class);//设置Mapper输出key类型
            job.setOutputValueClass(Text.class);//设置Mapper输出value类型
            FileInputFormat.addInputPath(job, new Path(args[0]));//输入文件目录
            FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出文件目录
            job.waitForCompletion(true);//参数true表示检查并打印 Job 和 Task 的运行状况
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
