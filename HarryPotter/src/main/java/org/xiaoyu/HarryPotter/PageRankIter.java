package org.xiaoyu.HarryPotter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class PageRankIter {
    public static class PageRankIterMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //Value格式 "Role0\tPR|Role1,x.xx|Role2,x.xx|...|Rolen,x.xx"
            String cur_line = value.toString();
            String []Items = cur_line.split("\t");
            //Items:["Role0", "PR|Role1,x.xx|Role2,x.xx|...|Rolen,x.xx"]
            int index = Items[1].indexOf("|");
            String list = Items[1].substring(index + 1);
            //list: "Role1,x.xx|Role2,x.xx|...|Rolen,x.xx"
            context.write(new Text(Items[0]), new Text(list));
            //Key:Role0 Value:Role1,x.xx|Role2,x.xx|...|Rolen,x.xx
            String []values = Items[1].split("\\|");
            //values: ["PR","Role1,x.xx",...,"Rolen,x.xx"]
            double last_PR = Double.parseDouble(values[0]);
            for(int i = 1; i < values.length; ++i) {
                String[] cur_value = values[i].split(",");
                double val = last_PR * Double.parseDouble(cur_value[1]);
                context.write(new Text(cur_value[0]), new Text(String.valueOf(val)));
                //根据PR_new_i = PR_last_0 * R_0_i + PR_last_1 * R_1_i + ... + PR_last_(n-1) * R_(n-1)_i
            }
        }
    }
    public static class PageRankIterCombiner extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double count = 0.0;
            for(Text value : values){
                if(value.toString().contains(",")){
                    context.write(key, value);
                    //包含list重新发出去
                }
                else{
                    count += Double.parseDouble(value.toString());
                    //否则累加
                }
            }
            context.write(key, new Text(String.valueOf(count)));
        }

    }
    public static class PageRankIterReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String value_list = "";
            double PR = 0.0;
            for(Text value : values){
                if(value.toString().contains(",")){
                    value_list = value.toString();
                }
                else{
                    PR += Double.parseDouble(value.toString());
                }
            }
            context.write(key, new Text(String.valueOf(PR) + "|" + value_list));
        }

    }

    public static void main(String[] args)
            throws Exception {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "PageRankIter");//设置环境参数
            job.setJarByClass(PageRankIter.class);//设置整个程序的类名
            job.setMapperClass(PageRankIterMapper.class);//设置Mapper类
            job.setCombinerClass(PageRankIterCombiner.class);//设置combiner类
            job.setReducerClass(PageRankIterReducer.class);//设置reducer类
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