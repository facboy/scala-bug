package org.facboy.idea.scala;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;

/**
 * @author Christopher Ng
 */
public class WordCountApp {
    private static final transient Logger logger = LoggerFactory.getLogger(WordCountApp.class);

    private static final Util UTIL = new Util();

    public static void main(String[] args) throws Exception {
        JavaStreamingContextFactory contextFactory = () -> {
            SparkConf conf = new SparkConf().setAppName("Word Count Application").setMaster("local[8]");
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            JavaStreamingContext sc = new JavaStreamingContext(conf, new Duration(5000L));

            JavaPairDStream<Character, String> map = sc.receiverStream(new HttpStringReceiver(StorageLevel.MEMORY_ONLY(), "/map"))
                    .filter(StringUtils::isNotBlank)
                    .mapToPair(line -> {
                        List<String> split = UTIL.spaceSplitter.splitToList(line);
                        logger.info("map: {}", split);
                        return new Tuple2<>(Character.toLowerCase(split.get(0).charAt(0)), split.get(1));
                    })
                    .updateStateByKey((mapWord, optionalState) -> {
                        if (mapWord.isEmpty()) {
                            return optionalState;
                        }
                        return Optional.of(mapWord.get(0));
                    });
            map.checkpoint(new Duration(5000L));

            JavaPairDStream<Character, String> wordsByFirstLetter = sc.receiverStream(new HttpStringReceiver(StorageLevel.MEMORY_ONLY(), "/words"))
                    .flatMap(line -> UTIL.spaceSplitter.split(line))
                    .map(word -> word.replaceAll("\\W+", ""))
                    .filter(StringUtils::isNotBlank)
                    .mapToPair(word -> {
                        logger.info("words: {}", word);
                        return new Tuple2<>(Character.toLowerCase(word.charAt(0)), word);
                    });

            wordsByFirstLetter
                    .join(map)
                    .mapToPair(firstLetterAndWords -> new Tuple2<>(firstLetterAndWords._2._2, firstLetterAndWords._2._1))
                    .updateStateByKey((words, stateOption) -> {
                        int count = 0;
                        if (stateOption.isPresent()) {
                            count = (Integer) stateOption.get();
                        }
                        return Optional.of(count + words.size());
                    })
                    .foreachRDD(rdd -> {
                        rdd.foreach(wordCount -> logger.info("{}: {}", wordCount._1, wordCount._2));
                        return null;
                    });

            sc.checkpoint("./poc.checkpoint");
            return sc;
        };

        JavaStreamingContext sc = JavaStreamingContext.getOrCreate("./poc.checkpoint", contextFactory);

        sc.start();
        sc.awaitTermination();
    }

    public static class Util implements Externalizable {
        public final transient Splitter spaceSplitter = Splitter.onPattern("\\s+");

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            logger.info("feh");
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }
    }
}
