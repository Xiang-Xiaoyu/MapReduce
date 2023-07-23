package org.xiaoyu.HarryPotter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.*;


public class LabelPropagationDriver {
    protected static void harryInit(String src_path, String des_path) throws IOException {
        //输入每行的格式 "Role0\t[R1,x.xx|R2,x.xx|...|Rn-1,x.xx]"
        //输出每行的格式 "Role0\tLabel[R1,x.xx|R2,x.xx|...|Rn-1,x.xx]"
        Path path_from = new Path(src_path);
        Path path_to = new Path(des_path);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path_from.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path_from);
        LineReader lineReader = new LineReader(inputStream, configuration);
        FSDataOutputStream outputStream = fileSystem.create(path_to);
        Text line = new Text();
        while (lineReader.readLine(line) > 0) {
            String str = line.toString();
            if (str == null || str.isEmpty()) {
                continue;
            }
            String[] str_list = str.split("\t");
            String out_str = str_list[0] + "\t" + str_list[0] + str_list[1] + "\n";
            outputStream.write(out_str.getBytes());
        }
        lineReader.close();
        outputStream.close();
    }

    protected static class HarryMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //输入Value的格式: "Role0\tLabel[R1,x.xx|R2,x.xx|...|Rn-1,x.xx]"
            String line = value.toString();
            int index1 = line.indexOf("\t");
            int index2 = line.indexOf("[");
            int index3 = line.indexOf("]");
            String name = line.substring(0, index1);//name为Role0
            String label = line.substring(index1 + 1, index2);//label是标签
            String name_list = line.substring(index2 + 1, index3);
            //name_list是R1,x.xx|R2,x.xx|...|Rn-1,x.xx
            StringTokenizer tokenizer = new StringTokenizer(name_list, "|");
            while (tokenizer.hasMoreTokens()) {
                String[] neighbor = tokenizer.nextToken().split(",");
                context.write(new Text(neighbor[0]), new Text("1#" + label + "#" + name));
            }
            context.write(new Text(name), new Text("2#" + name_list));
            /*发射
            * 第一组:Key:Role1 Value:"1#label#name"
            * 第二组:Key:name  Value:"2#name_list"
            * */
        }
    }

    protected static class HarryReducer extends Reducer<Text, Text, Text, Text> {
        HashMap<String, String> name2label_hash = new HashMap<>();
        //name2label_hash存储已经更新好label的<Role,label>

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //String label = "";
            //当前Key:Role 的label
            String name_list = "";
            //name_list记录R1,x.xx|R2,x.xx|...|Rn-1,x.xx
            HashMap<String, String> relation_name_label = new HashMap<>();
            //relation_name_label记录所有与当前的Key:Role1邻居的名字和标签 entry:<name,label>
            for (Text text : values) {
                String str = text.toString();
                if (str.charAt(0) == '1') {
                    String[] neighbor = str.split("#");
                    relation_name_label.put(neighbor[2], neighbor[1]);
                } else if (str.charAt(0) == '2') {
                    name_list = str.split("#")[1];
                }
            }

            HashMap<String, Double> label_rank_map = new HashMap<>();
            //label_rank_map存储label和对应的label总边权和
            StringTokenizer name_list_token = new StringTokenizer(name_list, "|");
            //name_list_token:["R1,x.xx","R2,x.xx",..."Rn-1,x.xx"]
            while (name_list_token.hasMoreTokens()) {
                String[] name_rank = name_list_token.nextToken().split(",");
                //name_rank:["Ri","x.xx"]
                Double current_rank = Double.parseDouble(name_rank[1]);
                //current_rank x.xx, 是矩阵中A_Ri_thisKey
                String current_label = "";
                //current_label表示["Ri","x.xx"]中Ri这个角色的label
                //current_label更新好了就拿更新的,没更新好,就拿当前的Key的Label
                if (name2label_hash.containsKey(name_rank[0])) {
                    current_label = name2label_hash.get(name_rank[0]);
                } else {
                    current_label = relation_name_label.get(name_rank[0]);
                }
                Double label_rank;
                //将所有相同label的边权加起来,没有则加进去,有则更新其值
                if ((label_rank = label_rank_map.get(current_label)) != null) {
                    label_rank_map.put(current_label, label_rank + current_rank);
                } else {
                    label_rank_map.put(current_label, current_rank);
                }
            }

            name_list_token = new StringTokenizer(name_list, "|");
            //重新分词,因为前面已经迭代遍历过了
            double max_rank = Double.MIN_VALUE;
            List<String> max_list = new ArrayList<>();
            while (name_list_token.hasMoreTokens()) {
                String[] neighbor = name_list_token.nextToken().split(",");
                //neighbor:["Ri","x.xx"]
                String current_label = "";
                //current_label Ri的label
                if (name2label_hash.containsKey(neighbor[0])) {
                    current_label = name2label_hash.get(neighbor[0]);
                } else {
                    current_label = relation_name_label.get(neighbor[0]);
                }
                double current_rank = label_rank_map.get(current_label);
                //current_rank表示当前的label的得分，以下加入得分高的key
                if (max_rank < current_rank) {
                    max_list.clear();
                    max_rank = current_rank;
                    max_list.add(neighbor[0]);
                } else if (max_rank == current_rank) {
                    max_list.add(neighbor[0]);
                }
            }

            Random random = new Random();
            int index = random.nextInt(max_list.size());
            String target_name = max_list.get(index);
            String target_label = "";
            String my_name = key.toString();
            if (name2label_hash.containsKey(target_name)) {
                target_label = name2label_hash.get(target_name);
            } else {
                target_label = relation_name_label.get(target_name);
                name2label_hash.put(my_name, target_label);
            }
            //防止出现单个孤立节点
            if (target_label == null) {
                target_label = my_name;
            }
            context.write(key, new Text(target_label + "[" + name_list + "]"));
        }
    }

    protected static void deletePath(String pathStr) throws IOException {
        //删除hdfs下对应路径的所有文件和目录
        Path path = new Path(pathStr);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path.getFileSystem(configuration);
        fileSystem.delete(path, true);
    }

    public static void main(String[] args)
            throws Exception {
        harryInit(args[0] + "part-r-00000", args[1] + "Data0/part-r-00000");
        int times = 20;
        int cur_time = 0;
        for (; cur_time < times; cur_time++) {
            String[] Itr = {args[1] + "Data" + (cur_time) + "/", args[1] + "Data" + (cur_time + 1) + "/"};
            try {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "LabelPropagation");
                job.setJarByClass(LabelPropagationDriver.class);
                job.setMapperClass(LabelPropagationDriver.HarryMapper.class);
                job.setReducerClass(LabelPropagationDriver.HarryReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(Itr[0]));
                FileOutputFormat.setOutputPath(job, new Path(Itr[1]));
                job.waitForCompletion(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (!Is_Continue(Itr)) {
                deletePath(Itr[0]);
                break;
            }
            deletePath(Itr[0]);
        }
        if (cur_time < times) {
            cur_time++;
        }
        String[] to_viewer = {args[1] + "Data" + (cur_time) + "/", args[1] + "Final_label/"};
        LabelPropagationViewer.main(to_viewer);
        deletePath(to_viewer[0]);
    }

    public static HashMap<String, String> get_label_list(String data_path)
            throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(data_path);
        FileSystem HDFS = path.getFileSystem(conf);
        FSDataInputStream inputStream = HDFS.open(path);
        LineReader lineReader = new LineReader(inputStream, conf);
        Text line = new Text();

        HashMap<String, String> res = new HashMap<>();
        while (lineReader.readLine(line) > 0) {
            String cur_line = lineReader.toString();
            String[] str1 = cur_line.split("\\[");
            if (str1.length == 2) {
                String[] str2 = str1[0].split("\t");
                if (str2.length == 2)
                    res.put(str2[0], str2[1]);
            }
        }
        HDFS.close();
        return res;
    }

    public static boolean Is_Continue(String[] args)
            throws IOException {
        HashMap<String, String> label_list1 = get_label_list(args[0] + "part-r-00000");
        HashMap<String, String> label_list2 = get_label_list(args[1] + "part-r-00000");
        int total = label_list1.size();
        int changed = 0;
        for (String name : label_list1.keySet()) {
            assert (label_list2.containsKey(name));
            if (!label_list1.get(name).equals(label_list2.get(name))) {
                changed++;
            }
        }
        Double proportion = Double.valueOf(changed) / Double.valueOf(total);
        if (proportion < 0.05) {
            return false;
        } else {
            return true;
        }
    }
}