package com.knoldus.KUP.Problem5;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Objects;

public class ParDoFiltering {

    private static final String CSV_HEADER = "month,price";
    public static void main(String[] args) {  // java main method

        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

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
        pipeline.run().waitUntilFinish();
        System.out.println("pipeline executed successFully");
    }

    private static class FilterBmwAndFordFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext processContext) {

            String[] tokens = Objects.requireNonNull(processContext.element()).split(",");
            if(tokens[0].equals("month") || tokens[0].equals("price"))
                processContext.output(processContext.element());
        }
    }

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
