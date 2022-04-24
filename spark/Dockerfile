FROM gettyimages/spark:2.4.0-hadoop-3.0

ARG SSH_MASTER_USER
ARG SSH_MASTER_PASS

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    nano \
    sudo \
    openssh-server \
    software-properties-common \
    gnupg 


COPY ssh_config /etc/ssh/ssh_config
COPY sshd_config /etc/ssh/sshd_config


RUN useradd -ms /bin/bash -g ssh $SSH_MASTER_USER 
COPY user.sh /usr/local/bin/user.sh
RUN chmod +x /usr/local/bin/user.sh
RUN /usr/local/bin/user.sh
RUN usermod --shell /bin/bash $SSH_MASTER_USER


COPY scripts/. /home/$SSH_MASTER_USER/scripts
COPY environment /home/$SSH_MASTER_USER/.ssh/environment

COPY requirement.txt /usr/scripts/requirement.txt
RUN python -m pip install -r /usr/scripts/requirement.txt

# RUN echo "export JAVA_HOME=/usr/jdk1.8.0_202; export HADOOP_HOME=/usr/hadoop-3.0.0; export HADOOP_CONF_DIR=/usr/hadoop-3.0.0/etc/hadoop; export SPARK_VERSION=2.4.0; export SPARK_HOME=/usr/spark-2.4.0; export SPARK_DIST_CLASSPATH=/usr/hadoop-3.0.0/etc/hadoop/*:/usr/hadoop-3.0.0/share/hadoop/common/lib/*:/usr/hadoop-3.0.0/share/hadoop/common/*:/usr/hadoop-3.0.0/share/hadoop/hdfs/*:/usr/hadoop-3.0.0/share/hadoop/hdfs/lib/*:/usr/hadoop-3.0.0/share/hadoop/hdfs/*:/usr/hadoop-3.0.0/share/hadoop/yarn/lib/*:/usr/hadoop-3.0.0/share/hadoop/yarn/*:/usr/hadoop-3.0.0/share/hadoop/mapreduce/lib/*:/usr/hadoop-3.0.0/share/hadoop/mapreduce/*:/usr/hadoop-3.0.0/share/hadoop/tools/lib/* ;" >> /home/$SSH_MASTER_USER/.bashrc

RUN echo "export PATH=$JAVA_HOME/bin:$PATH" >> ~/.bashrc


RUN chown -R $SSH_MASTER_USER:ssh /home/$SSH_MASTER_USER/scripts
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
 
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
 
CMD tail -f /dev/null