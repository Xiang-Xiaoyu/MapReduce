package org.xiaoyu.HarryPotter;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RelationGraphDriver {
    public static class HarryMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] name_times = value.toString().split("\t");
            //分词为["<name1,name2>", "times"]
            String name_temp = name_times[0].substring(1, name_times[0].length() - 1);
            //name_temp为"name1,name2"
            String[] names = name_temp.split(",");
            //names为["name1","name2"]
            String name1 = names[0];
            String name2 = names[1];
            String times = name_times[1];
            context.write(new Text(name1), new Text(name2 + "," + times));
            //发射Key:"name1",Value:"name2,times"
        }
    }

    public static class HarryCombiner extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //将一个节点上的相同的Key:name1的Value进行合并,制表符分隔
            if (values == null) return;
            String temp = "";
            for (Text val : values) {
                temp = temp + val.toString() + "\t";
            }
            context.write(key, new Text(temp));
        }
    }

    public static class HarryReduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (values == null) return;
            List<String> myValues = new ArrayList();
            //myValues 存储"name,times"数据,下面累加所有的times
            double sum = 0.0;
            for (Text val : values) {
                String[] people = val.toString().split("\t");
                for (String p : people) {
                    int times = Integer.parseInt(p.split(",")[1]);
                    sum = sum + times;
                    myValues.add(p);
                }
            }
            StringBuffer res = new StringBuffer();
            //创建一个StringBuffer对象来存储需要输出的字符串,最终格式为[name1,p1|name2,p2|...|namen,pn]
            res.append("[");
            for (String val : myValues) {
                String person = val.split(",")[0];
                res.append(person);
                res.append(",");
                double times = (double) Integer.parseInt(val.split(",")[1]);
                double pTimes = times / sum;
                res.append(pTimes);
                res.append("|");
            }
            int l = res.length();
            res.deleteCharAt(l - 1);
            //最后一个"|"需要删除
            res.append("]");
            context.write(key, new Text(res.toString()));
        }
    }

    public static void main(String args[])
            throws Exception {
        Configuration conf = new Configuration();
        Job job3 = Job.getInstance(conf, "RelationGraph");
        //设置作业入口
        job3.setJarByClass(RelationGraphDriver.class);
        //设置作业输入流
        job3.setInputFormatClass(TextInputFormat.class);
        //设置作业的各个类
        job3.setMapperClass(RelationGraphDriver.HarryMapper.class);
        job3.setCombinerClass(RelationGraphDriver.HarryCombiner.class);
        job3.setReducerClass(RelationGraphDriver.HarryReduce.class);
        //设置作业输出类
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        //设置Map输出类
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        //设置作业输入输出路径
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        //执行作业
        job3.waitForCompletion(true);
    }
}
