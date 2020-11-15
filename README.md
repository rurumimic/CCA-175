# CCA-175 practice

- [CCA Spark and Hadoop Developer](https://www.cloudera.com/about/training/certification/cca-spark.html) Exam
- [Dataset](https://github.com/proedu-organisation/CCA-175-practice-tests-resource)

---

## Practice

1. [Test 1](docs/01.test.md)
2. [Test 2](docs/02.test.md)
3. Test 3
4. Final

---

## Exam

Each user is given their own CDH6 (currently 6.1.1) cluster pre-loaded with Spark 2.4.

- Number of Questions: **8–12** performance-based (hands-on) tasks on Cloudera Enterprise cluster. See below for full cluster configuration
- Time Limit: **120 minutes**
- Passing Score: 70% (6 ~ 9 tasks)
- Language: English
- Price: USD $295

---

## Upload data to HDFS

```bash
sudo yum install git
git clone https://github.com/proedu-organisation/CCA-175-practice-tests-resource.git resource
hdfs dfs -copyFromLocal resource/retail_db db
```

![](https://github.com/proedu-organisation/CCA-175-practice-tests-resource/raw/master/images/Retail_DB.png)

```bash
categories/part-m-00000

customers/part-m-00000

customers-avro/part-m-00000.avro
customers-avro/part-m-00001.avro
customers-avro/part-m-00002.avro
customers-avro/part-m-00003.avro

customers-tab-delimited/part-m-00000

departments/part-m-00000

order_items/part-m-00000

orders/part-m-00000

orders_parquet/741ca897-c70e-4633-b352-5dc3414c5680.parquet

orders_permissions/part-m-00000
orders_permissions/part-m-00001
orders_permissions/part-m-00002
orders_permissions/part-m-00003

products/part-m-00000

products_avro/part-m-00000.avro
products_avro/part-m-00001.avro
products_avro/part-m-00002.avro
products_avro/part-m-00003.avro

products_sequencefile/part-00000
products_sequencefile/part-00001
```

---

## Start

### Download

```bash
mkdir share
cd share
curl -O https://archive.apache.org/dist/hadoop/core/hadoop-2.7.7/hadoop-2.7.7.tar.gz
curl -O https://downloads.apache.org/spark/spark-2.4.7/spark-2.4.7-bin-without-hadoop-scala-2.12.tgz
```

### VM

```bash
vagrant up
vagrant ssh
```

### Set

```bash
IP="192.168.33.10"

# Java
sudo yum install -y java-1.8.0-openjdk-devel
echo 'export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/jre' >> ~/.bash_profile

# Host
cat <<EOF | sudo tee /etc/hosts
127.0.0.1 localhost
::1 localhost
$IP my.example.com my
EOF
sudo hostnamectl set-hostname my

# SSH
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ssh-keyscan -t ecdsa localhost >> ~/.ssh/known_hosts
ssh-keyscan -t ecdsa my.example.com >> ~/.ssh/known_hosts
ssh-keyscan -t ecdsa $IP >> ~/.ssh/known_hosts
ssh-keyscan -t ecdsa 0.0.0.0 >> ~/.ssh/known_hosts
```

### Install Hadoop, Spark, Hive

```bash
mkdir ~/hadoop ~/spark ~/hive
tar xvzf /share/hadoop-2.7.7.tar.gz -C ~/hadoop --strip-components=1
tar xvzf /share/spark-2.4.7-bin-without-hadoop-scala-2.12.tgz -C ~/spark --strip-components=1
```

```bash
echo 'export HADOOP_HOME=/home/$USER/hadoop' >> ~/.bash_profile
echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> ~/.bash_profile
echo 'export PATH=$PATH:$HADOOP_HOME/sbin' >> ~/.bash_profile
echo 'export SPARK_HOME=/home/$USER/spark' >> ~/.bash_profile
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bash_profile
echo 'export PATH=$PATH:$SPARK_HOME/sbin' >> ~/.bash_profile
source ~/.bash_profile
```

#### Avro

```bash
curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/2.4.7/spark-avro_2.12-2.4.7.jar
mv spark-avro_2.12-2.4.7.jar $SPARK_HOME/jars
```

#### Hadoop

```bash
sed -i '25s/.*/export JAVA_HOME=\/usr\/lib\/jvm\/java-1.8.0-openjdk\/jre/' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```

```bash
cat <<EOF | tee $HADOOP_HOME/etc/hadoop/core-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://my.example.com:9000</value>
  </property>
</configuration>
EOF
```

```bash
cat <<EOF | tee $HADOOP_HOME/etc/hadoop/hdfs-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
EOF
```

```bash
cat <<EOF | tee $HADOOP_HOME/etc/hadoop/mapred-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOF
```

```bash
cat <<EOF | tee $HADOOP_HOME/etc/hadoop/yarn-site.xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
    <name>yarn.nodemanager.vmem-pmem-ratio</name>
    <value>5</value>
  </property>
</configuration>
EOF
```

#### Spark

```bash
mv $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
echo "export SPARK_MASTER_HOST=$IP" >> $SPARK_HOME/conf/spark-env.sh
echo 'export SPARK_DIST_CLASSPATH=$(hadoop classpath)' >> $SPARK_HOME/conf/spark-env.sh
echo 'export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop' >> $SPARK_HOME/conf/spark-env.sh
echo 'export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop' >> $SPARK_HOME/conf/spark-env.sh
```

### Format

```bash
# Clean up # rm -rf /tmp/hadoop-$USER
hdfs namenode -format
```

### Start HDFS, YARN

```bash
start-dfs.sh
start-yarn.sh
```

```bash
jps

6867 DataNode
7475 NodeManager
6734 NameNode
7038 SecondaryNameNode
7198 ResourceManager
7615 Jps
```

### Working directory

```bash
hdfs dfs -mkdir -p /user/$USER
hdfs dfs -ls -R /
```

### Start Spark Shell

```bash
spark-shell --master yarn --deploy-mode client --driver-memory 2g
```

```bash
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _  / __/   _/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.7
      /_/

Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 1.8.0_272)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```
