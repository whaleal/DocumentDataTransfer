FROM centos:centos7.9.2009
RUN yum install java-11-openjdk.x86_64 -y
MAINTAINER ddt-replay-oplog
ADD DDT /opt/DDT/
ADD start.sh /var/log/
RUN chmod 777 /var/log/start.sh
# 暴露相应端口
EXPOSE 80




