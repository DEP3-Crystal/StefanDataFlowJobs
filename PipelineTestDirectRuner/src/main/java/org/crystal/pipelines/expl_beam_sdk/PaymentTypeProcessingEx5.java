package org.crystal.pipelines.expl_beam_sdk;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Objects;

public class PaymentTypeProcessingEx5 {

    private static final String CSV_HEADER = "Date,Product,Price,Card,Country";

    public interface AveragePriceProcessingOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("src/main/resources/source/SalesJan2009.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.String("src/main/resources/sink/sinkSalesJan2009")
        @Validation.Required
        String getOutputFile();

        void setOutputFile(String value);
    }

    public static void main(String[] args) {

        AveragePriceProcessingOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AveragePriceProcessingOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeAveragePriceFn()))
                .apply("AverageAggregation", Mean.perKey())//generate a set of KV
                .apply("FormatResult", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Double> productCount) ->
                                productCount.getKey() + "," + productCount.getValue()))

                .apply("WriteResult",
                        TextIO.write()
                                .to(options.getOutputFile())
                                .withoutSharding()
                                .withSuffix(".csv")
                                .withHeader("Product,AveragePrice")
                                .withShardNameTemplate("-SSS")
                );


        pipeline.run().waitUntilFinish();

        System.out.println("Pipeline execution complete!");
    }

    private static class FilterHeaderFn extends DoFn<String, String> {

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

    private static class ComputeAveragePriceFn extends DoFn<String, KV<String, Double>> {

        @ProcessElement
        public void processElement(ProcessContext c) {

            String[] data = Objects.requireNonNull(c.element()).split(",");

            String product = data[1];
            Double price = Double.parseDouble(data[2]);
            System.out.println(product + "," + price);
            c.output(KV.of(product, price));
        }
    }

}
