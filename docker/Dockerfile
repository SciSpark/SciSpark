FROM maven:3

ENV SCALA_VERSION 2.11.8
ENV SBT_VERSION 0.13.12

# Install Scala
RUN curl -fsL http://www.scala-lang.org/files/archive/scala-$SCALA_VERSION.tgz | tar xfz - -C /root/

ENV PATH=/root/scala-$SCALA_VERSION/bin:${PATH}

# Install sbt
RUN \
  curl -fsL -o sbt-$SBT_VERSION.deb http://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb 

# Install tools
RUN apt-get update && apt-get install -y \
  apt-utils \
  git \
  sbt 
  
# Install Spark 2.0.0 prebuilt with Hadoop 2.7
RUN curl -fsL http://d3kbcqa49mib13.cloudfront.net/spark-2.0.0-bin-hadoop2.7.tgz | tar xfz - -C /root/

ENV SPARK_HOME /root/spark-2.0.0-bin-hadoop2.7
ENV PATH $SPARK_HOME/bin:${PATH}

RUN cd /root && \
	git clone https://github.com/SciSpark/SciSpark

WORKDIR /root/SciSpark
RUN sbt clean assembly
