package com.knoldus.KUP.Problem5;

import com.knoldus.KUP.Problem2.CarBeamTextIO;
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
       Write a Beam pipeline to read the given google_stock_20202.csv file and compute the average closing price of every month of year 2020. Save the output in another file with Header [month,avg_price]
        Output sample file-

        month,avg_price
        03,1032.42
        04,1134.56
 **/

public class GoogleStock_IO {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleStock_IO.class);

    // CSN_HEADER  initialized as private static
    private static final String CSV_HEADER = "month,avg_price";

    public static void main(String[] args) {  // main class called

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);          // object created as pipeline

            // applied pipeline to read the required file
        pipeline.apply("Read-Lines", TextIO.read().from("src/main/resources/sink2/google_stock_2020.csv"))
                .apply("Filter-Header", ParDo.of(new GoogleStock_IO.FilterHeaderFn(CSV_HEADER)))
                .apply("payment-extractor", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> Collections.singletonList(line.split(",")[0])))
                .apply("count-aggregation", Count.perElement())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(typeCount -> typeCount.getKey() + "," + typeCount.getValue()))

                // applied pipeline to write the required file
                .apply("WriteResult", TextIO.write()
                        .to("src/main/resources/sink2/google_avg_price.csv")
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("month,avg_price"));

        LOGGER.info("Executing pipeline");
        pipeline.run();
    }

    //TODO: keep it in another class not as inner class.
    private static class FilterHeaderFn extends DoFn<String, String> {
        private static final Logger LOGGER = LoggerFactory.getLogger(CarBeamTextIO.FilterHeaderFn.class);

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
