# MapReduce Applied Example

Move `dried_fruits.txt` and `mapreduce.wordcount.jar` files to Linux.

The `dried_fruits.txt` file must be on HDFS.

Open a folder on HDFS

```
hdfs dfs -mkdir /user/train/datasets/dried_fruits_input
```

Send `dried_fruits.txt` to the opened folder on HDFS

```
hdfs dfs -put dried_fruits.txt /user/train/datasets/dried_fruits_input
```

Let's check

```
hdfs dfs -ls /user/train/datasets/dried_fruits_input
```

Running a jar file with YARN

```
yarn jar mapreduce.wordcount.jar com.veribilimiokulu.mapreduce.MR2WordCount /user/train/datasets/dried_fruits_input /user/train/datasets/dried_fruits_out
```

Let's check

```
hdfs dfs -ls /user/train/datasets/dried_fruits_out
```

Let's read the resulting file

```
hdfs dfs -head /user/train/datasets/dried_fruits_out/part-r-00000
```