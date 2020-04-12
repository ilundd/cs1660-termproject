FROM ubuntu:latest

COPY . /apps
WORKDIR /apps
EXPOSE 6000

ENV DEBIAN_FRONTEND noninteractive
ENV DEBIAN_FRONTEND teletype

RUN apt-get -y -q update && \
    apt-get -y -q install sudo && \
    apt-get -y -q install apt-utils && \
    sudo apt-get -y -q install default-jre && \
    sudo apt-get -y -q install software-properties-common htop && \
    sudo add-apt-repository ppa:linuxuprising/java && \
    sudo apt-get -y -q update && \
    sudo echo oracle-java13-installer shared/accepted-oracle-license-v1-2 select true | /usr/bin/debconf-set-selections && \
    sudo apt-get -y -q install oracle-java13-installer && \
    sudo update-java-alternatives -s java-13-oracle && \
    sudo apt-get -y -q install libunirest-java-java && \
    sudo apt-get update

RUN sudo apt -y -q update && \
    sudo apt -y -q install maven



ENV JAVA_HOME /usr/lib/jvm/java-13-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH


ADD "target/cs1660-termproject-1.0-jar-with-dependencies.jar" termproject.jar
ENTRYPOINT ["java", "-jar", "termproject.jar"]
