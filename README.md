程序在集群上要正确执行要保证以下几点

1. 输入路径下有正确的文件person_name_list.txt和其他文件等
2. 输出路径`/user/gr181860111/output-lab5`存在且为空
3. 命令为`yarn jar Lab05.jar`



输入文件路径在hdfs上的目录为

```
/user/gr181860111/data_test/exp5/
```



以下解释本目录下所有文件的基本信息

HarryPotter

是一个完整的maven工程,已经clean过了

源文件都在以下目录中:

```
/HarryPotter/src/main/java/org/xiaoyu/HarryPotter
```



结果输出文件

包含了五个任务的输出结果



一些中间数据

是实验中用于分析的数据



Lab05.jar

是打包好的jar包，已经指定了程序入口和输入输出文件路径



MPLab5-181860111-项仁浩.pdf

为程序设计报告



实验报告.md

为程序设计报告的Markdown版本



验收PPT.pptx

为验收的PPT



组员的信息

包含了完成本实验的参与人