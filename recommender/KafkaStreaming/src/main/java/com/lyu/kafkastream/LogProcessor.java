package com.lyu.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @Description: 日志流处理函数，需要继承接口 Processor, 该接口有泛型
 * @ClassName: LogProcessor
 * @Author: klaus
 * @Date: 2019-12-26 11:44
 **/


/**
 * 传输的内容是 byte, 网络传输中的数据需要是序列化的
 */
public class LogProcessor implements Processor<byte[], byte[]> {
    // TODO: 主要重写 init 和 process 即可, init 有参数 context 上下文
    private ProcessorContext processorContext;

    @Override
    public void init(ProcessorContext context){
        this.processorContext = context;
    }

    @Override
    public void process(byte[] dummy, byte[] line) {
        // 核心处理流程
        String stringLine = new String(line);
        System.out.println("stringline" + stringLine);
        if (stringLine.contains("PRODUCT_RATING_PREFIX:")){
            System.out.println("product rating data coming!!!!" + stringLine);
            stringLine = stringLine.split("PRODUCT_RATING_PREFIX:")[1].trim();
            // Forwards a key/value pair to the downstream processors
            processorContext.forward("logProcessor".getBytes(), stringLine.getBytes());
        }

    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
