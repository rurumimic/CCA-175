# CCA-175 practice

- [CCA Spark and Hadoop Developer](https://www.cloudera.com/about/training/certification/cca-spark.html) Exam
- [Dataset](https://github.com/proedu-organisation/CCA-175-practice-tests-resource)

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

### Install Hadoop, Spark

```bash
mkdir ~/hadoop ~/spark
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
    <value>hdfs://$IP:9000</value>
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
hdfs namenode -format
```

### Start

```bash
start-dfs.sh
start-yarn.sh
```

```bash
jps
```

### Working directory

```bash
hdfs dfs -mkdir -p /user/$USER
hdfs dfs -ls -R /
```

---

## Exam

Each user is given their own CDH6 (currently 6.1.1) cluster pre-loaded with Spark 2.4.

- Number of Questions: **8–12** performance-based (hands-on) tasks on Cloudera Enterprise cluster. See below for full cluster configuration
- Time Limit: **120 minutes**
- Passing Score: 70% (6 ~ 9 tasks)
- Language: English
- Price: USD $295

### Exam Question Format
Each CCA question requires you to solve a particular scenario. In some cases, a tool such as **Impala** or **Hive** may be used. In most cases, coding is required.

### Evaluation, Score Reporting, and Certificate
Your exam is graded immediately upon submission and you are e-mailed a score report within three days of your exam. Your score report displays the problem number for each problem you attempted and a grade on that problem. If you fail a problem, the score report includes the criteria you failed (e.g., “Records contain incorrect data” or “Incorrect file format”). We do not report more information in order to protect the exam content. Read more about reviewing exam content on the FAQ.

If you pass the exam, you receive a second e-mail within a week of your exam with your digital certificate as a PDF and your license number.

### Audience and Prerequisites
There are **no prerequisites required** to take any Cloudera certification exam. The CCA Spark and Hadoop Developer exam (CCA175) follows the same objectives as [Cloudera Developer Training for Spark and Hadoop](https://www.cloudera.com/about/training/courses/developer-training-for-spark-and-hadoop.html) and the training course is an excellent preparation for the exam. 

### Required Skills

- Transform, Stage, and Store: Load data from HDFS for use in Spark applications
- Data Analysis: Use Spark SQL to interact with the metastore
- Configuration: change your application configuration, such as increasing available memory
