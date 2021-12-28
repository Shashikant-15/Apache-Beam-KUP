package com.knoldus.KUP.Problem5;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.logging.Logger;
import static com.knoldus.KUP.Problem5.ParDoFiltering.FilterBmwAndFordFn.LOGGER;

public class ParDoFiltering {

    private static final String CSV_HEADER = "month,price";

    public static void main(String[] args) {    // java main method

        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        // applied pipeline to read the required file
        pipeline.apply("ReadingFile", TextIO.read().from("src/main/resources/sink2/google_stock_2020.csv"))
                .apply("FilterHeader", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)))
                .apply("FilterPrice", ParDo.of(new com.knoldus.KUP.Problem5.ParDoFiltering.FilterPriceFn(10000.0)))
                .apply("printToConsole", ParDo.of(new DoFn<String, Void>() {

                    @ProcessElement
                    public void processElement(ProcessContext processContext){
                        System.out.println(processContext.element());
                    }
                }));
        // run the pipeline for execution
        pipeline.run().waitUntilFinish();
        LOGGER.info("pipeline executed successFully");
    }
    static class FilterBmwAndFordFn extends DoFn<String, String> {
        public static final Logger LOGGER = (Logger) LoggerFactory.getLogger(FilterBmwAndFordFn.class);
        @ProcessElement
        public void processElement(ProcessContext processContext) {

            String[] tokens = Objects.requireNonNull(processContext.element()).split(",");
            if(tokens[0].equals("month") || tokens[0].equals("price"))
                processContext.output(processContext.element());
        }
    }

    // method extended to  DoFn
    private static class FilterPriceFn extends DoFn<String, String> {

        private final Double price;
        FilterPriceFn(Double price) {
            this.price = price;
        }

        @ProcessElement
        public void processElement(ProcessContext processContext) {

            String[] tokens =  Objects.requireNonNull(processContext.element()).split(",");
            double carPrice = Double.parseDouble(tokens[1]);
            if (carPrice != 0 && carPrice <= price)
                processContext.output(processContext.element());
        }

    }
}
