package org.xiaoyu.HarryPotter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;

public class PageRankDriver {

    public static ArrayList<Double> getPR(String dataPath) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(dataPath);
        FileSystem HDFS = path.getFileSystem(conf);
        //得到HDFS文件系统
        FSDataInputStream inputStream = HDFS.open(path);
        //打开相应路径的文件得到输入流
        LineReader lineReader = new LineReader(inputStream, conf);
        //对输入流新建行读取器
        Text line = new Text();

        ArrayList<Double> res = new ArrayList<>();
        //以下将一各个人物图节点对应的PageRank值给顺序存储到res中
        while (lineReader.readLine(line) > 0) {
            String record = line.toString().split("\t")[1];
            String[] values = record.split("\\|");
            Double PR = Double.parseDouble(values[0]);
            res.add(PR);
        }
        HDFS.close();
        return res;
    }

    public static boolean IsContinue(String[] args) throws IOException {
        //判断PageRank迭代是否要继续下去
        ArrayList<Double> PR_list1 = getPR(args[0] + "/part-r-00000");
        ArrayList<Double> PR_list2 = getPR(args[1] + "/part-r-00000");
        for (int i = 0; i < PR_list1.size(); ++i) {
            //只要仍然存在一组点的PageRank更新差值大于一个极小值,则说明还没达到收敛条件
            if (Math.abs(PR_list1.get(i) - PR_list2.get(i)) > 0.0001) {
                return true;
            }
        }
        return false;
    }

    public static void deletePath(String pathStr) throws IOException {
        //在hdfs系统删除路径对应的文件
        Path path = new Path(pathStr);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path.getFileSystem(configuration);
        fileSystem.delete(path, true);
    }

    public static void main(String[] args) throws Exception {
        String[] GB = {args[0], args[1] + "/tmp0"};
        GraphBuilder.main(GB);//建图初始化
        String[] Itr = {"", ""};
        int times = 20;//预定迭代次数
        int cur_time = 0; //初始化次数
        for (; cur_time < times; ++cur_time) {
            Itr[0] = args[1] + "/tmp" + (cur_time); //上一次迭代结果所在文件夹路径
            Itr[1] = args[1] + "/tmp" + (cur_time + 1);//当前迭代结果所在文件夹路径
            PageRankIter.main(Itr);//进行当前迭代计算PR值
            if (!IsContinue(Itr)) {
                //如果收敛则退出循环
                deletePath(Itr[0]);
                break;
            }
            deletePath(Itr[0]);
        }
        if (cur_time < times) {
            //这意味着break退出,最后需要处理的输出文件则为cur_time+1
            cur_time++;
        }
        String[] RV = {args[1] + "/tmp" + (cur_time), args[1] + "/FinalRank"};
        RankViewer.main(RV); //进行结果排序
        deletePath(RV[0]);
    }

}