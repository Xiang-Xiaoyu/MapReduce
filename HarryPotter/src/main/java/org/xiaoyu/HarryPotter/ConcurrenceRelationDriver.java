package org.xiaoyu.HarryPotter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ConcurrenceRelationDriver {
    //这个类实现单词同现统计
    public static class HarryMapper extends Mapper<Object, Text, Text, IntWritable> {
        public static Map<String, String> nickname_table = new HashMap<String, String>();

        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSystem hdfs = FileSystem.get(context.getConfiguration());
            //创建hdfs文件系统
            Path rPath = new Path(FilePath.nickName);
            //设置路径为预设的别名表路径
            BufferedReader in = new BufferedReader(new InputStreamReader(hdfs.open(rPath)));
            //创建读取对象
            String str = in.readLine();
            //接下来按行读取，一行就是一个人的所有名称，一行中的第一个是统一名称
            while (str != null) {
                String[] current = str.split("\t");
                for (int i = 1; i < current.length; ++i) {
                    nickname_table.put(current[i], current[0]);
                    //后面的词是别名，最前面的词是统一后的名称
                }
                str = in.readLine();
            }
            in.close();
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> names = new HashMap<>();
            //names存储一段中所有名字归一化后的统一名字和响应数量
            //如果是Ron Ron Ron Harry Harry 那么<Ron,Harry>和<Harry,Ron>同现为3*2次
            String[] allNames = value.toString().split("\t");
            //各个名字之间之间用制表符连接,防止名字本身有空格
            String temp = "";
            for (String name : allNames) {
                temp = nickname_table.get(name);
                if (temp == null) {
                    temp = name;
                    //如果在别名哈希表中没有找到,说明是统一后的名字
                }
                //下面统计各个名字以及其出现的次数
                if (names.keySet().contains(temp)) {
                    int newInt = names.get(temp).intValue() + 1;
                    names.put(temp, new Integer(newInt));
                } else {
                    names.put(temp, new Integer(1));
                }
            }
            for (String name1 : names.keySet()) {
                for (String name2 : names.keySet()) {
                    if (!name1.equals(name2)) {
                        int nameFreq1 = names.get(name1).intValue();
                        int nameFreq2 = names.get(name2).intValue();
                        context.write(new Text("<" + name1 + "," + name2 + ">"), new IntWritable(nameFreq1 * nameFreq2));
                        //<1,2>和<2,1>都是需要发射的
                    }
                }
            }
            names.clear();
        }
    }

    public static class HarryCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //单个节点上求和Key的Value,减少传输次数
            if (values == null) return;
            int sum = 0;
            for (IntWritable val : values) {
                sum = sum + val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class HarryReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            if (values == null) return;
            int sum = 0;
            for (IntWritable val : values) {
                sum = sum + val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String args[])
            throws Exception {
        Configuration conf = new Configuration();
        Job job2 = Job.getInstance(conf, "ConcurrenceRelation");
        //设置作业入口
        job2.setJarByClass(ConcurrenceRelationDriver.class);
        //设置作业输入
        job2.setInputFormatClass(TextInputFormat.class);
        //设置作业各个类
        job2.setMapperClass(ConcurrenceRelationDriver.HarryMapper.class);
        job2.setCombinerClass(ConcurrenceRelationDriver.HarryCombiner.class);
        job2.setReducerClass(ConcurrenceRelationDriver.HarryReduce.class);
        //设置作业输出类
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        //设置Map输出类
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        //设置输入输出路径
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        //执行作业
        job2.waitForCompletion(true);
    }
}