package com.knoldus.KUP.Problem5;

import com.knoldus.KUP.Problem2.CarMemoryClass;
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

public class GoogleBeamMemory {

        // LOGGER initializes as private static final
    private static final Logger LOGGER = LoggerFactory.getLogger(CarMemoryClass.class);

    //  main method called
    public static void main(String[] args) {

        PipelineOptions options;
        final List<String> list = Arrays.asList(
                "11,1743.3900024",
                "09,1511.502377571",
                "03,1116.3422309999999",
                "06,1431.5954645454547",
                "10,1541.9208984090908",
                "04,1230.1114327142855",
                "12,1767.7109042272727",
                "08,1545.0195253809525",
                "05,1381.37250975",
                "07,1515.317732409091");

        options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

            // applied pipeline to the required file
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
                        LOGGER.info(input);  // for LOGGER inputs
                        return null;
                    }
                }));


            // run pipeline to  required file
        pipeline.run().waitUntilFinish();
    }
}
