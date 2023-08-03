if [ ! -f "mongodbT.pid" ]; then
  echo "没有启动任何实例程序"
  exit
fi
# 检查是否有实例启动pid
pid=$(cat mongodbT.pid)
if [ ${#pid} -gt 0 ]; then
  kill -9 ${pid}
  echo "${pid}程序已关闭"
  echo "" > mongodbT.pid
else
  echo "无运行中的程序关闭"
fi
