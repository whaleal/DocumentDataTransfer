echo "                              _ _   _____
  _ __  ___ _ _  __ _ ___  __| | |_|_   _|
 | '  \/ _ \ ' \/ _' / _ \/ _' | '_ \| |
 |_|_|_\___/_||_\__, \___/\__,_|_.__/|_|
                |___/                       "
echo "启动"
# 若没有mongodbT.pid文件 则创建
if [ ! -f "mongodbT.pid" ]; then
  touch "mongodbT.pid"
fi
# 检查是否有实例启动pid
pid=$(cat mongodbT.pid)
if [ ${#pid} -gt 0 ]; then
  echo "有其他已经运行的MongodbT任务请检查"
  exit
fi

nohup java -jar ../execute-1.0-SNAPSHOT.jar ../config/mongodbT.properties >/dev/null 2>&1 &
echo $! >mongodbT.pid
