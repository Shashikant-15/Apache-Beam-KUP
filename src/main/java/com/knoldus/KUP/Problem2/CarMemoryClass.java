package com.knoldus.KUP.Problem2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class CarMemoryClass {

    private static final Logger LOGGER = LoggerFactory.getLogger(CarMemoryClass.class);
    public static void main(String[] args) {

        PipelineOptions options;
        final List<String> list = Arrays.asList("Ford,15500",
        "Toyota,52400",
        "TATA,6500",
                "Maruti,6500",
        "Nissan,16600",
        "Renault,10500",
        "Mercedes-Benz,21500",
        "Mercedes-Benz,22700",
        "Nissan,20447.154");

        options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(Create.of(list)).setCoder(StringUtf8Coder.of())
                .apply("print-before", MapElements.via(new SimpleFunction<String, String>() {
                    @Override
                    public String apply(String input) {
                        LOGGER.info(input);
                        return input;
                    }
                }))
                .apply("payment-extractor", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> Collections.singletonList(line.split(",")[1])))
                .apply("count-aggregation", Count.perElement())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(typeCount -> typeCount.getKey() + ":" + typeCount.getValue()))
                .apply("print-after", MapElements.via(new SimpleFunction<String, Void>() {
                    @Override
                    public Void apply(String input) {
                        LOGGER.info(input);
                        return null;
                    }
                }));

       pipeline.run().waitUntilFinish();
    }

}
