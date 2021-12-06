package com.knoldus.KUP.Problem2;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 Write a Beam pipeline to read the given CSV file of car ad data and compute the average price of different Cars. Save the output in another CSV file with headers [car avg_price]
 Output sample file-

 car,avg_price
 Infiniti,29900.0
 Ford,3500.0
 Tesla,176900.0
 TATA,112900.0
 */
public class CarBeamTextIO {      // created own method  CarBeamTextIO for i/o

    private static final Logger LOGGER = LoggerFactory.getLogger(CarBeamTextIO.class);

    private static final String CSV_HEADER = "car,price";

    // main method called
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // applied pipeline to read the required file
        pipeline.apply("Read-Lines", TextIO.read().from("src/main/resources/Car_file.csv"))
                .apply("Filter-Header", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("payment-extractor", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> Collections.singletonList(line.split(",")[0])))
                .apply("count-aggregation", Count.perElement())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(typeCount -> typeCount.getKey() + "," + typeCount.getValue()))

                // applied pipeline to write result on  the required file
                .apply("WriteResult", TextIO.write()
                        .to("src/main/resources/sink/Car_Avg_price.csv.csv")
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("Car,Price"));

        LOGGER.info("Executing pipeline");    // pipeline execute
        pipeline.run();
    }

    //TODO: keep it in another class not as inner class.
    public static class FilterHeaderFn extends DoFn<String, String> {
        private static final Logger LOGGER = LoggerFactory.getLogger(FilterHeaderFn.class);

        private final String header;

        FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext processContext) {
            String row = processContext.element();
            assert row != null;
            if (!row.isEmpty() && !row.contains(header))
                processContext.output(row);
            else
                LOGGER.info("Filtered out the header of the csv file: [{}]", row);

        }
    }
}
