package com;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

/**
 * Created by Administrator on 2017/12/20.
 */
public class ProducerPartition {
    public static void main(String[] args) {
        ArrayList name = new ArrayList<String>();
        name.add(0,"jie");
        name.add(1,"xuan");
        name.add(2,"jian");
        name.add(3,"sen");

        int MAX_AGE = 60;

        Random random = new Random();

        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "172.20.1.104:9092,172.20.1.105:9092");
        props.put("partitioner.class", "com.SimplePartitioner");

        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));

        String topic = "sparkStreaming";

        StringBuilder stringBuilder = new StringBuilder();
        int id = 0;
        while(true){
            id = id + 1 ;
            int i = random.nextInt(name.size());
            String nameStr = (String) name.get(i);
            int ageStr = random.nextInt(MAX_AGE);
            stringBuilder.append(id + "/");
            stringBuilder.append(nameStr + "/");
            stringBuilder.append(ageStr);
            System.out.println(stringBuilder);
            producer.send(new KeyedMessage<String, String>(topic,""+id,stringBuilder.toString()));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stringBuilder.delete( 0, stringBuilder.length() );
        }


    }
}
