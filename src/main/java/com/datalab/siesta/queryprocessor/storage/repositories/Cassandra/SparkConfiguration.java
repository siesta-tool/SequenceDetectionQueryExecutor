package com.datalab.siesta.queryprocessor.storage.repositories.Cassandra;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

/**
 * Contains the configuration of Spark in order to connect to ScyllaDB database
 */
@Configuration
@PropertySource("classpath:application.properties")
@ConditionalOnExpression("'${database}' == 'cassandra' and '${delta}' == 'false'")
public class SparkConfiguration {

    @Value("${app.name:siesta2}")
    private String appName;

    @Value("${master.uri:local[*]}")
    private String masterUri;

    @Value("${cassandra.contact.points:cassandra}")
    private String cassContactPoints;

    @Value("${scylla.port:9042}")
    private String cassPort;

    @Value("${scylla.keyspace:siesta}")
    private String cassKeyspace;

    @Value("${spring.data.cassandra.user:cassandra}")
    private String cassUser;

    @Value("${spring.data.cassandra.password:cassandra}")
    private String cassPassword;

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
                .setAppName(appName)
                .setMaster(masterUri)
                .set("spark.driver.extraJavaOptions", "--add-opens java.base/sun.security.action=ALL-UNNAMED")
                .set("spark.executor.extraJavaOptions", "--add-opens java.base/sun.security.action=ALL-UNNAMED")
                .set("spark.driver.maxResultSize", "5g")
                // ScyllaDB/Cassandra specific configurations
                .set("spark.cassandra.connection.host", cassContactPoints)
                .set("spark.cassandra.connection.port", cassPort)
                .set("spark.cassandra.connection.keep_alive_ms", "60000")
                .set("spark.cassandra.connection.timeout_ms", "30000")
                .set("spark.cassandra.read.timeout_ms", "30000")
//                .set("spark.cassandra.connection.reconnection_delay_ms.base", "1000")
                .set("spark.cassandra.connection.reconnection_delay_ms.max", "60000")
                .set("spark.cassandra.auth.username", cassUser)
                .set("spark.cassandra.auth.password", cassPassword)
                .set("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions");
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(this.sparkConf());
    }

    @Bean
    public SparkSession sparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .sparkContext(this.javaSparkContext().sc())
                .appName("siesta 2")
                .getOrCreate();

        // Configure Spark SQL for Cassandra/ScyllaDB
        spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
        spark.conf().set("spark.sql.adaptive.enabled", "true");
        spark.conf().set("spark.sql.adaptive.coalescePartitions.enabled", "true");

        return spark;
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}
