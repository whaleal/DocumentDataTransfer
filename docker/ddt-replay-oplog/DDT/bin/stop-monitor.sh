# 关闭tomcat
echo "开始关闭monitor"
sh ../apache-tomcat-8.5.78/bin/shutdown.sh

echo "关闭monitor-controller"
pkill -f monitor-controller-1.0-SNAPSHOT
