package com.github.aseara.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Created by aseara on 2017/6/1.
 *
 */
public class KafkaNetworkWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("KafkaNetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        //ssc.sparkContext().addJar("E:\\study\\spark\\spark-streaming-java\\out\\" +
        //        "artifacts\\spark_streaming_java_jar\\spark-streaming-java.jar");

        // Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("192.168.2.191", 9999);

        JavaDStream<String> words = lines.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(SPACE.split(x)));
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);


        wordCounts.print();

        ssc.start();
        ssc.awaitTermination();
    }

}
