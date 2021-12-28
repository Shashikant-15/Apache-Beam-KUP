package com.knoldus.KUP.Problem2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Objects;

public class ParDoFiltering {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParDoFiltering.class);
    // CSV HEADER initialize as String
    private static final String CSV_HEADER = "car,price";
    public static void main(String[] args) {              // java main method called

        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        // applied pipeline to read the required file
        pipeline.apply("ReadingFile", TextIO.read().from("src/main/resources/Car_file.csv"))
                .apply("FilterHeader", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)))
                .apply("FilterBMWandFord", ParDo.of(new FilterBmwAndFordFn()))
                .apply("FilterPrice", ParDo.of(new FilterPriceFn(10000.0)))
                .apply("printToConsole", ParDo.of(new DoFn<String, Void>() {

                    @ProcessElement
                    public void processElement(ProcessContext processContext){
                        System.out.println(processContext.element());
                    }
                }));
        pipeline.run().waitUntilFinish();
        LOGGER.info("pipeline executed successFully");
    }

    public static class FilterBmwAndFordFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext processContext) {

            // check the tokens condition for output
            String[] tokens = Objects.requireNonNull(processContext.element()).split(",");
            if(tokens[0].equals("Ford") || tokens[0].equals("Tesla") || tokens[0].equals("TATA") || tokens[0].equals("Infiniti"))
                processContext.output(processContext.element());
        }
    }

        // Filtration extended to DoFn
    public static class FilterPriceFn extends DoFn<String, String> {

        private final Double price;

        public FilterPriceFn(Double price) {
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
