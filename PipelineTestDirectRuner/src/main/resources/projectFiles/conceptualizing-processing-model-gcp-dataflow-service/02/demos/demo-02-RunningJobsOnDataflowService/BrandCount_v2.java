package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.*;

public class BrandCount {

    private static final String CSV_HEADER = "id,price,brand,model,year,title_status," +
            "mileage,color,vin,lot,state,country,condition";


    public static void main(String[] args) {

        ReadCsvFileOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadCsvFileOptions.class);

        processData(options);
    }

    public interface ReadCsvFileOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Validation.Required
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.String("gs://loony-dataflow-storage/Sink/brandCount")
        String getOutput();

        void setOutput(String value);
    }


    static void processData(ReadCsvFileOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()))
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
                                .to(options.getOutput())
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