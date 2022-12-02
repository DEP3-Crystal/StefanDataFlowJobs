package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.*;

public class BrandCount {

    private static final String CSV_HEADER = "id,price,brand,model,year,title_status," +
            "mileage,color,vin,lot,state,country,condition";


    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        processData(options);

    }

    static void processData(PipelineOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadLines", TextIO.read().from(
                            "gs://loony-dataflow-storage/Source/USA_cars_datasets.csv"))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("ExtractBrand", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> Collections.singletonList(line.split(",")[2])))
                .apply("CountAggregation", Count.perElement())
                .apply("FormatResult", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> typeCount) ->
                                typeCount.getKey() + "," + typeCount.getValue()))
                .apply("WriteResult",
                        TextIO.write()
                                .to("gs://loony-dataflow-storage/Sink/brandCount")
                                .withoutSharding()
                                .withSuffix(".csv")
                                .withShardNameTemplate("-SSS")
                                .withHeader("Brand,Count"));

        pipeline.run().waitUntilFinish();
        System.out.println("Pipeline execution complete!");
    }


    private static class FilterHeaderFn extends DoFn<String, String> {

        private static final long serialVersionUID = 1L;

        private final String header;

        public FilterHeaderFn(String header) {
            this.header = header;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String row = c.element();

            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            }
        }
    }
}