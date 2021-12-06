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
public class GoogleStock_IO {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleStock_IO.class);

    private static final String CSV_HEADER = "month,avg_price";

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read-Lines", TextIO.read().from("src/main/resources/sink2/google_stock_2020.csv"))
                .apply("Filter-Header", ParDo.of(new GoogleStock_IO.FilterHeaderFn(CSV_HEADER)))
                .apply("payment-extractor", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> Collections.singletonList(line.split(",")[0])))
                .apply("count-aggregation", Count.perElement())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(typeCount -> typeCount.getKey() + "," + typeCount.getValue()))
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
