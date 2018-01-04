package com;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.util.Random;

/**
 * Created by Administrator on 2017/12/20.
 */
public class SimplePartitioner implements Partitioner {
    public SimplePartitioner(VerifiableProperties verifiableProperties) {
    }

    public int partition(Object key, int numPartitions) {
        int partition = 0;

        String k = (String) key;

        partition = Math.abs(k.hashCode()) % numPartitions;

        return partition;
    }
}
