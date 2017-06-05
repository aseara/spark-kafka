package com.github.aseara.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by aseara on 2017/6/1.
 *
 */
public class KafkaNetworkWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("KafkaNetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        //JavaReceiverInputDStream<String> lines =
        //         KafkaUtils.createDirectStream(scc, )

        String zkQuorum = "192.168.2.191:2181";
        String group = "1";
        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put("test", 1);

        JavaPairInputDStream<String, String> lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap);

        JavaDStream<String> words = lines.flatMap((FlatMapFunction<Tuple2<String, String>, String>)
                x -> Arrays.asList(SPACE.split(x._2())));

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);


        wordCounts.print();

        ssc.start();
        ssc.awaitTermination();
    }

}
