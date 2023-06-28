#!/bin/bash

# 指定目录路径
dir="/Users/ouyanghongrong/github-projects/disalg.dbcdc/store-base/dbcdc rw tidb opt SI (SI) "

# 列出所有子目录
subdirs=$(find "$dir" -type d)

# 遍历每个子目录
for subdir in $subdirs; do
  # 查找 history.json 文件
  if [ -f "$dir/$subdir/history.json" ]; then
    # 获取文件的绝对路径并打印
    abs_path=$(readlink -f "$dir/$subdir/history.json")
    echo "Found history.json at $abs_path"
    java -jar ./target/SI-Checker-jar-with-dependencies.jar -equalVIS true -historyPath /Users/ouyanghongrong/github-projects/disalg.dbcdc/tidb/template/new-history-template.json
  fi
done