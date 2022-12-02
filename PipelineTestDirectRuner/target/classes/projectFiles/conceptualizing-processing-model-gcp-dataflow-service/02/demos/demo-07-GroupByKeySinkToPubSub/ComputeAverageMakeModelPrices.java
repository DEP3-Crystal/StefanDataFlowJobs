package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

public class ComputeAverageMakeModelPrices {

    private static final String CSV_HEADER =
            "car,price,body,mileage,engV,engType,registration,year,model,drive";

    public interface GcsOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Validation.Required
        String getInputFilesLocation();
        void setInputFilesLocation(String value);

        @Description("PubSub topic to write to")
        @Validation.Required
        String getTopicName();
        void setTopicName(String value);
    }

    public static void main(String[] args) {

        GcsOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(GcsOptions.class);

        performAverageComputation(options);
    }

    static void performAverageComputation(GcsOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadAds", TextIO.read().from(options.getInputFilesLocation()))
                .apply("FilterHeader", ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("MakeModelPriceKVFn", ParDo.of(new MakeModelPriceKVFn()))
                .apply("MakeGrouping", GroupByKey.create())
                .apply("ComputeAveragePrice", ParDo.of(new ComputeAveragePriceFn()))
                .apply("CreatePubSubMessage", ParDo.of(new ConvertToMessage()))
                .apply("WriteToPubSub", PubsubIO.writeStrings().to(
                    "projects/loony-dataflow-review/topics/" + options.getTopicName()));

        pipeline.run().waitUntilFinish();
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

    private static class MakeModelPriceKVFn extends DoFn<String, KV<String, Double>> {

        private static final long serialVersionUID = 1L;
 
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] fields = c.element().split(",");

            String make = fields[0];
            String model = fields[8];

            Double price = Double.parseDouble(fields[1]);

            c.output(KV.of(make + "-" + model, price));
        }
    }

    
    private static class ComputeAveragePriceFn extends
            DoFn<KV<String, Iterable<Double>>, KV<String, Double>> {

        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(
                @Element KV<String, Iterable<Double>> element,
                OutputReceiver<KV<String, Double>> out) {

            String make = element.getKey();

            int count = 0;
            double sumPrice = 0;

            for (Double price: element.getValue()) {
                sumPrice +=  price;
                count++;
            }

            out.output(KV.of(make, sumPrice / count));
        }
    }

    public static class ConvertToMessage extends DoFn<KV<String, Double>, String> {

        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(DoFn<KV<String, Double>, String>.ProcessContext c) {
            c.output(c.element().getKey() + " now at an average price of: " + c.element().getValue());
        }
    }
}
