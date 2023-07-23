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
public class LabelPropagationViewer {
    public static class HarryMapper extends Mapper<Object,Text,Text,Text> {
        public void map(Object key,Text value,Context context)
                throws IOException, InterruptedException{
            String cur_line = value.toString();
            int index1= cur_line.indexOf("\t");
            int index2= cur_line.indexOf("[");
            String name=cur_line.substring(0,index1);
            String label=cur_line.substring(index1+1,index2);
            context.write(new Text(label),new Text(name));
        }
    }
    public static class HarryReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException, InterruptedException{
            String output="";
            for(Text value:values){
                output=output+value.toString()+" ";
            }
            context.write(key,new Text(output));
        }
    }
    public static void main(String[] args){
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Task5_Viewer");
            job.setJarByClass(LabelPropagationViewer.class);
            job.setMapperClass(LabelPropagationViewer.HarryMapper.class);
            job.setReducerClass(LabelPropagationViewer.HarryReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}