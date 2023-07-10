# HDFS Basic Commands

Initialising all services

```
start-all.sh
```

HDFS root directory listing `-ls`

```bash
hdfs dfs -ls /
```

List HDFS user directory

```bash
hdfs dfs -ls /user
```

List the directory of the user HDSF train

```bash
hdfs dfs -ls /user/train
```

HDFS long extension:

```bash
hdfs dfs -ls hdfs://localhost:9000/user/train
```

Create a folder named `big_data` on HDFS `-mkdir`

```bash
hdfs dfs -mkdir /user/train/big_data
```

Copying a file from Local Linux to HDFS. `-put`

```bash
hdfs dfs -put ~/datasets/Advertising.csv /user/train/big_data/Advertising.csv
```

Moving files on HDFS `-mv`

```bash
hdfs dfs -mv /user/train/big_data/Advertising.csv /user/train
```

File copying on HDFS `-cp`

```bash
hdfs dfs -cp /user/train/Advertising.csv /user/train/big_data
```

Change the group of a file on HDFS: `-chown`

```bash
 hdfs dfs -chown train:train /user/train/big_data/Advertising.csv
```

Move a file on HDFS to the Local Linux home directory

```bash
hdfs dfs -get /user/train/Advertising.csv
```

Delete a file on HDFS

```bash
hdfs dfs -rm -skipTrash /user/train/Advertising.csv
```

**Note:** Add `-skipTrash` to delete directly without sending to trash.

**Note:** If you want to delete a folder, add `-r`.

```bash
hdfs dfs -rm -r -skipTrash /user/train/big_data
```

Reading a file on HDFS:

- `head` - initial observations

```bash
hdfs dfs -head /user/train/Advertising.csv
```

- `tail` - last observations

```
hdfs dfs -tail /user/train/Advertising.csv
```
