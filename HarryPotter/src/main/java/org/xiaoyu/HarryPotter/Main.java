package org.xiaoyu.HarryPotter;

public class Main {
    public static void main(String[] args) throws Exception {
        //五个Driver类分别是五个任务的驱动类,以下依次调用驱动,参数输入路径和输出路径
        //需要注意的是,task2依赖于task1,task3依赖于task2,task4和task5都依赖于task3
        try {
            String[] task1 = {FilePath.inputLoc, FilePath.task1Loc};
            NameProcessDriver.main(task1);

            String[] task2 = {FilePath.task1Loc, FilePath.task2Loc};
            ConcurrenceRelationDriver.main(task2);

            String[] task3 = {FilePath.task2Loc, FilePath.task3Loc};
            RelationGraphDriver.main(task3);

            String[] task4 = {FilePath.task3Loc, FilePath.task4Loc};
            PageRankDriver.main(task4);

            String[] task5 = {FilePath.task3Loc, FilePath.task5Loc};
            LabelPropagationDriver.main(task5);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}