package crystal.ex.expl_beam_sdk;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Collections;

public class PaymentTypeProcessingEx4 {

    private static final String CSV_HEADER = "Date,Product,Price,Card,Country";

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadLines", TextIO.read().from("src/main/resources/source/SalesJan2009.csv"))
                .apply(ParDo.of(new FilterHeaderFn(CSV_HEADER)))
                .apply("ExtractPaymentTypes", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> Collections.singletonList(line.split(",")[3])))
                .apply("CountAggregation", Count.perElement())
                .apply("FormatResult", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> typeCount) ->
                                typeCount.getKey() + "," + typeCount.getValue()))
                .apply("WriteResult",
                        TextIO.write()
                                .to("src/main/resources/sink/payment_type_count")
                                .withoutSharding()
                                .withSuffix(".csv")
                                .withShardNameTemplate("-SSS")
                                .withHeader("Card,Count"));

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

            assert row != null;
            if (!row.isEmpty() && !row.equals(this.header)) {
                c.output(row);
            }
        }
    }

}
