package com.grafysi.horizpipes.utils.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;


/**
 * Extracts the topic name from Kafka connect record and insert to header.
 */
public class ExtractTopicName<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractTopicName.class);

    private static final int CACHE_SIZE = 100;

    public static final String TOPIC_REGEX = "topic.regex";
    public static final String HEADER_NAME = "header.name";
    public static final String HEADER_VALUE_FORMAT = "header.value.format";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC_REGEX, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH,
                    "The regex used for extracting words from topic name")
            .define(HEADER_NAME, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH,
                    "The name of the new header")
            .define(HEADER_VALUE_FORMAT, ConfigDef.Type.STRING,
                    NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH,
                    "The format of the value of the new header, can be formed by word group extracted from "
                            + TOPIC_REGEX + " using $n where n is group index starting from 1");

    private Pattern topicRegex;

    private String headerName;

    private String headerValueFormat;

    private Cache<String, String> headerValueCache;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return null;
    }

    @Override
    public void configure(Map<String, ?> props) {
        var config = new SimpleConfig(CONFIG_DEF, props);
        topicRegex = Pattern.compile(config.getString(TOPIC_REGEX));
        headerName = config.getString(HEADER_NAME);
        headerValueFormat = config.getString(HEADER_VALUE_FORMAT);
        headerValueCache = new SynchronizedCache<>(new LRUCache<>(CACHE_SIZE));
    }

    @Override
    public void close() {

    }

    @Override
    public R apply(R record) {
        var topic = record.topic();

        var headerValue = formHeaderValueWith(topic);

        //LOGGER.debug("Header value: {}", headerValue);

        if (headerValue == null) {
            return record;
        }

        var updatedHeaders = record.headers().duplicate();
        updatedHeaders.add(headerName, Values.parseString(headerValue));

        LOGGER.info("ExtractTopic applied and will be returned. Record topic: {}", record.topic());

        return record.newRecord(
                topic,
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                updatedHeaders);
    }

    private String[] extractWords(Pattern pattern, String input) {
        var matcher = pattern.matcher(input);
        if (!matcher.matches()) {
            return null;
        }

        return IntStream.rangeClosed(0, matcher.groupCount())
                .mapToObj(matcher::group)
                .toArray(String[]::new);
    }

    private String formHeaderValueWith(String topic) {
        var cachedValue = headerValueCache.get(topic);
        if (cachedValue != null) {
            return cachedValue;
        }

        var words = extractWords(topicRegex, topic);

        //LOGGER.debug("Extracted word groups: {}", Arrays.toString(words));

        if (words == null) {
            return null;
        }

        var valueBuilder = new StringBuilder();

        boolean escapeCurrent = false;

        for (int i = 0; i < headerValueFormat.length(); i++) {
            var currentChar = headerValueFormat.charAt(i);

            if (escapeCurrent) {
                valueBuilder.append(currentChar);
                escapeCurrent = false;

            } else if (currentChar == '\\') {

                escapeCurrent = true;

            } else if (currentChar == '$') {
                var wordIndex = getIntValueAt(
                        i + 1, headerValueFormat, 0, words.length - 1);
                if (wordIndex == -1) {
                    LOGGER.warn("Cannot inject words for value format {}. Process as null value.",
                            headerValueFormat);
                    return null;
                }

                valueBuilder.append(words[wordIndex]);
                i++;

            } else {
                valueBuilder.append(currentChar);
            }
        }

        var headerValue = valueBuilder.toString();

        headerValueCache.put(topic, headerValue);

        return headerValue;
    }

    private int getIntValueAt(int index, String input, int lowerLimit, int upperLimit) {
        if (index >= input.length()) {
            return -1;
        }

        var ch = input.charAt(index);
        var num = Character.getNumericValue(ch);
        return num >= Math.max(0, lowerLimit) && num <= Math.min(9, upperLimit) ? num : -1;
    }

}














