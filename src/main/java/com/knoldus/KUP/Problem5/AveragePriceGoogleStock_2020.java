package com.knoldus.KUP.Problem5;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class AveragePriceGoogleStock_2020 {

    private static final String CSV_HEADER = "Date,Open,High,Low,Close,Adj Close,Volume";

    public static void main(String[] args) {


        final AveragePriceProcessingOptions averagePriceProcessingOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AveragePriceProcessingOptions.class);

        Pipeline pipeline = Pipeline.create(averagePriceProcessingOptions);

        pipeline.apply("Read-Lines", TextIO.read()
                        .from(averagePriceProcessingOptions.getInputFile()))
                .apply("Filter-Header", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)))
                .apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(line.split("-")[1], Double.parseDouble(tokens[4]));
                        }))
                .apply("AverageAggregation", Mean.perKey())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(stockCount -> stockCount.getKey() + "," + stockCount.getValue()))
                .apply("WriteResult", TextIO.write()
                        .to(averagePriceProcessingOptions.getOutputFile())
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("month,Avg_price"));

        pipeline.run();
        System.out.println("pipeline executed successfully");
    }

    public interface AveragePriceProcessingOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("src/main/resources/sink2/google_stock_2020.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write")
        @Default.String("src/main/resources/sink2/google_avg_price.csv")
        String getOutputFile();

        void setOutputFile(String value);
    }
}

