echo "  ____    ____    _____ "
echo " |  _ \\  |___ \\  |_   _|"
echo " | | | |   __) |   | |  "
echo " | |_| |  / __/    | |  "
echo " |____/  |_____|   |_|  "
echo "                        "

echo "启动"
# 若没有 DDT.pid 文件则创建
if [ ! -f "DDT.pid" ]; then
  touch "DDT.pid"
fi
# 检查是否有实例启动 pid
pid=$(cat DDT.pid)
if [ ${#pid} -gt 0 ]; then
  echo "有其他已经运行的 DDT 任务请检查"
  exit
fi

# 获取主机内存大小（以MB为单位）
total_memory=$(free -m | grep 'Mem:' | awk '{print $2}')
# 计算最大堆内存大小为主机内存的80%
max_heap_size=$((total_memory * 80 / 100))M

nohup java -Xmx$max_heap_size -jar ../execute-sync-1.0-SNAPSHOT.jar ../config/DDT.properties >/dev/null 2>&1 &
echo $! >DDT.pid
