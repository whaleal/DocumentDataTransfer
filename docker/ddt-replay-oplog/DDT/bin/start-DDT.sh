#!/bin/bash

echo "  ____    ____    _____ "
echo " |  _ \\  |___ \\  |_   _|"
echo " | | | |   __) |   | |  "
echo " | |_| |  / __/    | |  "
echo " |____/  |_____|   |_|  "
echo "                        "

echo "开始启动DDT数据传输工具"

# 检查是否有实例启动 pid
pid=$(pgrep -f "replayOplog")
if [ ${#pid} -gt 0 ]; then
  echo "已经运行的DDT任务,启动命令退出"
  exit
fi

source_url=$1
target_url=$2
startop=$3
endop=$4
ns=$5
wapURL=$6

sed -i "s#url1#$source_url#g" /opt/DDT/config/DDT.properties

sed -i "s#url2#$target_url#g" /opt/DDT/config/DDT.properties

sed -i "s#startop#$startop#g" /opt/DDT/config/DDT.properties

sed -i "s#endop#$endop#g" /opt/DDT/config/DDT.properties

sed -i "s#oplogSaveNs#$ns#g" /opt/DDT/config/DDT.properties

echo "wapURL=$6" >> /opt/DDT/config/DDT.properties


# 获取主机内存大小（以MB为单位）
total_memory=$(free -m | grep 'Mem:' | awk '{print $2}')
# # 计算最大堆内存大小为主机内存的80%
max_heap_size=$((total_memory * 80 / 100))M
echo "获取本机:Max Heap Size: $max_heap_size"


java -Xmx$max_heap_size -jar /opt/DDT/replayOplog.jar /opt/DDT/config/DDT.properties

# 等待10秒
sleep 5

# 检查 execute-1.0-SNAPSHOT 进程是否存活
pid=$(pgrep -f "replayOplog")
if [ ${#pid} -eq 0 ]; then
  echo "DDT数据传输工具未能成功启动"
else
  echo "DDT数据传输工具启动成功"
fi
