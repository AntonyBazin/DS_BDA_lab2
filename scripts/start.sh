#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify database name!'
    exit 1
fi

export PATH=$PATH:/usr/local/hadoop/bin/
hdfs dfs -rm -r logs
hdfs dfs -rm -r out

# Устанавливаем Cassandra
apt install -y pgp
curl -OLk https://dlcdn.apache.org/cassandra/3.0.29/apache-cassandra-3.0.29-bin.tar.gz
gpg --print-md SHA256 apache-cassandra-3.0.29-bin.tar.gz
# curl -L https://dlcdn.apache.org/cassandra/3.0.29/apache-cassandra-3.0.29-bin.tar.gz.sha256
tar xvzf apache-cassandra-3.0.29-bin.tar.gz
cd apache-cassandra-3.0.29 || exit 126
bin/cassandra
cd /
#sudo systemctl status cassandra
#tail -f logs/system.log
#bin/nodetool status
#cd ..
#apt-get install --reinstall ca-certificates
#echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
#curl https://downloads.apache.org/cassandra/KEYS | sudo apt-key add -
#sudo apt-get update
#sudo apt-get install cassandra=3.11.11-1
#tail -f logs/system.log
#sudo service cassandra start
#nodetool status
#sudo apt-get update -y
#sudo apt-get install -y postgresql postgresql-contrib

# Устанавливаем Python, cqlsh
apt update
apt install -y build-essential zlib1g-dev libssl-dev libncurses5-dev libsqlite3-dev libreadline-dev libtk8.6 libgdm-dev libdb4o-cil-dev libpcap-dev
apt install -y build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev wget
#curl -OLk https://www.python.org/ftp/python/3.10.10/Python-3.10.10.tgz
#tar -zxvf Python-3.10.10.tgz
#cd Python-3.10.10 || exit 127
#./configure
#make install
#make altinstall
apt install -y python 3.8

pip3 install six

cd /
tar -xzvf cqlsh-5.1.34-bin.tar.gz



# Создаем таблицу c данными перелетов
echo "----------- Starting working with the DB -----------"
#cqlsh-5.1.34/bin/cqlsh -e 'ALTER USER cassandra PASSWORD '\''1234'\'';'
cqlsh-5.1.34/bin/cqlsh -e 'DROP KEYSPACE if exists '"$1"';'
cqlsh-5.1.34/bin/cqlsh -e 'CREATE KEYSPACE '"$1"' WITH REPLICATION = {'\''class'\'' : '\''SimpleStrategy'\'', '\''replication_factor'\'' : 1 };'
cqlsh-5.1.34/bin/cqlsh -e 'CREATE TABLE '"$1"'.flights (id UUID PRIMARY KEY,
flight int,
dep_time text,
arr_icao text,
dep_icao text,
arr_tz text,
dep_tz text,
);'

# Добавляем входные данные в таблицу
while IFS= read -r line; do
    echo "Text read from file: $line"
    default_IFS="$IFS"
    IFS=","
    declare -a fields=($line)
#    echo "Field 0: ${fields[0]}"
#    echo "Field 1: ${fields[1]}"
#    echo "Field 2: ${fields[2]}"
#    echo "Field 3: ${fields[3]}"
#    echo "Field 4: ${fields[4]}"
#    echo "Field 5: ${fields[5]}"
    echo 'INSERT INTO '"$1"'.flights (id, flight, dep_time, arr_icao, dep_icao, arr_tz, dep_tz) values
                 (uuid(), '"${fields[0]}"', '"${fields[1]}"', '"${fields[2]}"', '"${fields[3]}"', '"${fields[4]}"', '"${fields[5]}"');'
    cqlsh-5.1.34/bin/cqlsh -e 'INSERT INTO '"$1"'.flights (id, flight, dep_time, arr_icao, dep_icao, arr_tz, dep_tz) values
        (uuid(), '"${fields[0]}"', '\'''"${fields[1]}"''\'', '\'''"${fields[2]}"''\'', '\'''"${fields[3]}"''\'', '\'''"${fields[4]}"''\'', '\'''"${fields[5]}"''\'');'
    IFS=default_IFS
done < test_batch

echo "----------- Data inserted into DB -----------"

# Send new data to DFS
hdfs dfs -put input input
# yarn jar target/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar input output 60
hdfs dfs -get output output

#export PATH=$PATH:/sqoop-1.4.7.bin__hadoop-2.6.0/bin
#
## Скачиваем Spark
#if [ ! -f spark-2.3.1-bin-hadoop2.7.tgz ]; then
#    wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
#    tar xvzf spark-2.3.1-bin-hadoop2.7.tgz
#else
#    echo "Spark already exists, skipping..."
#fi
#export SPARK_HOME=/spark-2.3.1-bin-hadoop2.7
#export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
#export PATH=$PATH:/spark-2.3.1-bin-hadoop2.7/bin
#spark-submit --class bdtc.lab2.SparkSQLApplication --master local --deploy-mode client --executor-memory 1g --name wordcount --conf "spark.app.id=SparkSQLApplication" /tmp/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://127.0.0.1:9000/user/root/logs/ out
#echo "DONE! RESULT IS: "
#hadoop fs -cat  hdfs://127.0.0.1:9000/user/root/out/part-00000
