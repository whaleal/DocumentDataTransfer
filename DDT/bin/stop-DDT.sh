
echo "开始关闭DDT"
pkill -f execute-1.0-SNAPSHOT

# 等待一段时间，确保进程有足够时间关闭
sleep 1

# 再次检查是否还有 execute-1.0-SNAPSHOT 进程
pid=$(pgrep -f "execute-1.0-SNAPSHOT")
if [ ${#pid} -eq 0 ]; then
  echo "DDT关闭成功"
else
  echo "DDT关闭未成功"
fi
