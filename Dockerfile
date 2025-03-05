FROM ubuntu:20.04

#ENV JAVA_HOME="/usr/lib/jvm/default-jvm/"

RUN apt-get update && apt-get install -y openjdk-17-jdk maven wget && \
    echo "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))" >> /etc/profile.d/java.sh
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:${JAVA_HOME}/bin
ENV JARS_DIR=/code/src/main/resources/jars


# Install maven
#RUN apk add --no-cache maven

WORKDIR /code

# Prepare by downloading dependencies
ADD pom.xml /code/pom.xml
RUN mvn dependency:resolve

# Adding source, compile and package into a fat jar
ADD src /code/src
RUN test -f ${JARS_DIR}/hadoop-aws-3.3.4.jar || wget -P ${JARS_DIR} https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN test -f ${JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar || wget -P ${JARS_DIR} https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
RUN test -f ${JARS_DIR}/hadoop-client-3.3.4.jar || wget -P ${JARS_DIR} https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client/3.3.4/hadoop-client-3.3.4.jar
RUN mvn clean compile package -f pom.xml -DskipTests

# Making sure jars are where they should be


CMD ["java", "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED" , "-jar", "target/siesta-query-processor-3.0.jar"]
#ENTRYPOINT ["tail", "-f", "/dev/null"]
