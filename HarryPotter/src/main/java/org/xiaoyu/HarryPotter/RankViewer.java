package org.xiaoyu.HarryPotter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class RankViewer {
    public static class RankViewerMapper extends Mapper<Object, Text, DoubleWritable, Text> {
        @Override

        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] split0 = value.toString().split("\t");
            String[] split1 = split0[1].split("\\|");
            context.write(new DoubleWritable(Double.parseDouble(split1[0])), new Text(split0[0]));
            //发射Key:PageRank值,Value:Name
        }
    }

    public static class RankViewerReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(text, key);
            }
        }
    }

    public static class DecDoubleCompare extends WritableComparator {
        public DecDoubleCompare() {
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -a.compareTo(b);
            //compareTo小于返回-1,大于返回1,完全相等返回0
            //所以compare方法大于返回-1,小于返回1,完全相等返回0,从大到小排序
        }
    }


    public static void main(String[] args)
            throws Exception {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "RankViewer");//设置环境参数
            job.setJarByClass(RankViewer.class);//设置整个程序的类名
            job.setSortComparatorClass(DecDoubleCompare.class);//设置比较器来排序结果
            job.setMapperClass(RankViewerMapper.class);//设置Mapper类
            job.setReducerClass(RankViewerReducer.class);//设置reducer类
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);//设置输出key类型
            job.setOutputValueClass(DoubleWritable.class);//设置输出value类型
            FileInputFormat.addInputPath(job, new Path(args[0]));//输入文件目录
            FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出文件目录
            job.waitForCompletion(true);//参数true表示检查并打印 Job 和 Task 的运行状况
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

