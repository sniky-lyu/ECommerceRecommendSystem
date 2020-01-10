package com.lyu.kafkastream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * @Description: 主程序,做日志数据的预处理，过滤筛选出需要的内容
 * @ClassName: Application
 * @Author: klaus
 * @Date: 2019-12-25 13:46
 **/
public class Application {
    public static void main(String[] args) {
        // 定义 broker 和 zookeeper
        String brokers = "192.168.13.128:9092";
        String zookeepers = "192.168.13.128:2181";
        // 定义输入和输出的 topic
        String from = "logs";    /*数据流来源，flume获取埋点日志*/
        String to = "recommender";  /*kafka中提供给spark_kafka的ConsumerStrategies消费获得评分数据流*/
        // 以上4个String变量在java运行jar命令时可以重新指定

        // 定义 kafka stream 配置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);
        // 时间戳异常的话需要加上下面这个属性，并且自定义时间错提取器继承TimestampExtractor
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);
        // 創建 kafka stream 配置對象
        StreamsConfig streamsConfig = new StreamsConfig(settings);
        // 定义拓扑构建器: 管道流程结构
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // 输入源topic --> 日志处理函数 --> 输出topic
        // TODO: 日志流处理函数定义 LogProcessor。将topic为“log”的信息流获取来做处理，并以“recommender”为新的topic转发出去
        topologyBuilder.addSource("SOURCE", from)
                .addProcessor("PROCESSOR", () -> new LogProcessor(), "SOURCE")
                .addSink("SINK", to, "PROCESSOR");
        // 创建 kafka stream
        KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder, streamsConfig);
        kafkaStreams.start();
        System.out.println("kafka stream started.");

    }

}
