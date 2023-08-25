#!/bin/bash
echo "  ____    ____    _____ "
echo " |  _ \\  |___ \\  |_   _|"
echo " | | | |   __) |   | |  "
echo " | |_| |  / __/    | |  "
echo " |____/  |_____|   |_|  "
echo "                        "

echo "开始启动monitor"
echo ""
rm -rf ../apache-tomcat-8.5.78/

echo "开始解压tomcat"
tar -zxf ../apache-tomcat-8.5.78.tar.gz -C ../

echo "DDT_WEB"
tar -zxf ../DDT_WEB.tar.gz -C ../apache-tomcat-8.5.78/webapps/

# 读取DDT.properties配置文件中的bind_ip属性
echo "读取DDT.properties配置文件中的bind_ip属性"
bind_ip=$(grep 'bind_ip=' ../config/DDT.properties | cut -d'=' -f2)

if [ -z "$bind_ip" ]; then
  echo "未找到bind_ip属性或属性值为空"
  echo "退出执行启动monitor"
  exit 1
fi

echo "替换bind_ip"
# 遍历../dist目录下的所有文件
find ../apache-tomcat-8.5.78/webapps/DDT_WEB/ -type f -print0 | while IFS= read -r -d '' file; do
  # 替换文件中的ddt.com为bind_ip
  sed -i "s/whalealDDT\.com/$bind_ip/g" "$file"
done
echo "替换完成"



# 启动jar
echo "启动monitor-controller"
nohup java -Xmx256M -jar ../monitor-controller-1.0-SNAPSHOT.jar --logPath=../logs/ &


echo "启动tomcat"
# 启动tomcat
sh ../apache-tomcat-8.5.78/bin/startup.sh
echo ""


# 检查是否有实例启动 pid
pid=$(pgrep -f "monitor-controller-1.0-SNAPSHOT")
if [ ${#pid} -gt 0 ]; then
  echo "请访问: http://$bind_ip:58000/DDT_WEB/#/home"
else
  echo "无法启动 monitor-controller"
  exit
fi
