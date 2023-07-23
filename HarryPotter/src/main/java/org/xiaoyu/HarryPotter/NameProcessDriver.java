package org.xiaoyu.HarryPotter;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.List;

public class NameProcessDriver {
    public static class NameProcessMapper extends Mapper<Object, Text, Text, NullWritable> {
        @Override
        protected void setup(Context context) throws IOException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path in = new Path(FilePath.personName);
            FSDataInputStream fsIn = fs.open(in);
            LineReader lineIn = new LineReader(fsIn, context.getConfiguration());
            Text line = new Text();
            while (lineIn.readLine(line) > 0) {
                CustomDictionary.insert(line.toString().trim(), "userDefine");
            }
            fsIn.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<Term> terms = HanLP.segment(value.toString());
            StringBuilder sb = new StringBuilder();
            int cnt = 0;
            for (Term i : terms) {
                if (i.nature.toString().equals("userDefine")) {
                    cnt++;
                    sb.append(i.word + "\t");
                }
            }
            if (0 != cnt) {
                context.write(new Text(sb.toString().trim()), NullWritable.get());
            }
        }
    }

    public static class NameProcessReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable it : values) {
                context.write(key, it.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //设置环境参数
        Job job = Job.getInstance(conf, "NameProcess");
        //设置程序类名
        job.setJarByClass(NameProcessDriver.class);
        //为作业设置map类
        job.setMapperClass(NameProcessDriver.NameProcessMapper.class);
        //为作业设置Reduce类
        job.setReducerClass(NameProcessDriver.NameProcessReducer.class);
        //设置Map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置Reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] listStatus = fs.listStatus(new Path(args[0]));
        //遍历目录下的所有文件，跳过person_name_list.txt
        for (FileStatus file : listStatus) {
            if (file.getPath().getName().startsWith("person")) {
                continue; //跳过person_name_list.txt
            }
            FileInputFormat.addInputPath(job, file.getPath());
        }
        //设置输出文件路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true); /* 启动作业 */
    }
}