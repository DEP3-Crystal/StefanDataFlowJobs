package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;

public class ExtractMakeModelPrices {

    private static final String CSV_HEADER =
            "car,price,body,mileage,engV,engType,registration,year,model,drive";

    public interface GcsOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Validation.Required
        String getInputFilesLocation();
        void setInputFilesLocation(String value);

        @Description("Name of BigQuery table to write to")
        @Validation.Required
        String getTableName();
        void setTableName(String value);
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
                .apply("ConvertToTableRow", ParDo.of(new FormatForBigQuery()))
                .apply("WriteToBigQuery", 
                    BigQueryIO.writeTableRows().to("loonycorn-dataflow-288106:results." + options.getTableName())
                        .withSchema(FormatForBigQuery.getSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

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

    public static class FormatForBigQuery extends DoFn<KV<String,Double>, TableRow> {

        private static final long serialVersionUID = 1L;

        public static final String makeModel = "make-model";
        public static final String average = "price";

        @ProcessElement
        public void processElement(DoFn<KV<String, Double>, TableRow>.ProcessContext c) {
            c.output(new TableRow()
                    .set(makeModel, c.element().getKey())
                    .set(average, c.element().getValue())
            );
        }

        static TableSchema getSchema() {
            return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
                {
                    add(new TableFieldSchema().setName(makeModel).setType("STRING"));
                    add(new TableFieldSchema().setName(average).setType("FLOAT"));
                }
            });
        }
    }


}
