package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.json.JSONObject;

import java.util.ArrayList;

public class Windowing {

    public static final String DATETIME = "time";
    public static final String COMPANY_NAME = "company";
    public static final String CLOSE = "close";
    public static final String AVERAGE_CLOSE = "average_close";
    public static final String TABLE_NAME = "tumblingWindowRawData";

    public interface PubSubToBqOptions extends PipelineOptions, StreamingOptions {

        @Description("The Cloud Pub/Sub topic to read from.")
        @Validation.Required
        String getInputTopic();
        void setInputTopic(String value);

        @Description("Output file's window size in number of seconds.")
        @Default.Integer(1)
        Integer getWindowSize();
        void setWindowSize(Integer value);

        @Description("The BigQuery table to write to.")
        @Validation.Required
        String getTableName();
        void setTableName(String value);
    }

    public static void main(String[] args) {
        PubSubToBqOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation().as(PubSubToBqOptions.class);
        
        options.setStreaming(true);

        performTumblingWindowing(options);
    }

    static void performTumblingWindowing(PubSubToBqOptions options) {
        
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> pubSubMessages = pipeline.apply(PubsubIO.readStrings()
            .fromTopic(options.getInputTopic())
            .withTimestampAttribute(DATETIME));
        
        pubSubMessages.apply("ApplyWindow", Window.<String>into(FixedWindows.of(
                    Duration.standardSeconds(options.getWindowSize())))
                .withTimestampCombiner(TimestampCombiner.EARLIEST))
                .apply("MapKeysToValues", ParDo.of(new ConvertToKV()))
                .apply("GroupByKeys", GroupByKey.create())
                .apply("CalculateAverage", ParDo.of(new Average()))
                .apply("ConvertToTableRows", ParDo.of(new ConvertToTableRow()))
                .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                            .to("loony-dataflow-review:results." + options.getTableName())
                        .withSchema(ConvertToTableRow.getSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pubSubMessages.apply("RawDataToTableRow", ParDo.of(new ConvertToRawTableRow()))
                .apply("WriteRawDataToBigQuery", BigQueryIO.writeTableRows()
                    .to("loony-dataflow-review:results." + TABLE_NAME)
                .withSchema(ConvertToRawTableRow.getSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        
        pipeline.run().waitUntilFinish();
    }

    public static class ConvertToKV extends DoFn<String, KV<String, Double>> {

        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(DoFn<String, KV<String, Double>>.ProcessContext c) {
            JSONObject obj = new JSONObject(c.element());

            c.output(KV.of(obj.getString(COMPANY_NAME), 
                           obj.getDouble(CLOSE)));
        }
    }
   
    public static class ConvertToRawTableRow extends DoFn<String, TableRow> {

        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(DoFn<String, TableRow>.ProcessContext c) {
            String input = c.element();

            JSONObject obj = new JSONObject(input);
            String dateTime = c.timestamp().toString();
            c.output(new TableRow()
                    .set(DATETIME, dateTime.substring(0, dateTime.length() - 1))
                    .set(COMPANY_NAME, obj.getString(COMPANY_NAME))
                    .set(CLOSE, obj.getDouble(CLOSE))
            );
        }

        public static TableSchema getSchema() {
            return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
                {
                    add(new TableFieldSchema().setName(DATETIME).setType("DATETIME"));
                    add(new TableFieldSchema().setName(COMPANY_NAME).setType("STRING"));
                    add(new TableFieldSchema().setName(CLOSE).setType("FLOAT"));
                }
            });
        }
    }

    public static class Average extends DoFn<KV<String, Iterable<Double>>, KV<String, Double>> {

        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(DoFn<KV<String, Iterable<Double>>, KV<String, Double>>.ProcessContext c) {
            double total = 0;
            int n = 0;
            
            for (Double i : c.element().getValue()) {
                total += i;
                n++;
            }
            Double average = total / n;

            c.output(KV.of(c.element().getKey(), average));
        }
    }

    public static class ConvertToTableRow extends DoFn<KV<String, Double>, TableRow> {

        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(DoFn<KV<String, Double>, TableRow>.ProcessContext c) {

            String dateTime = c.timestamp().toString();
            c.output(new TableRow()
                    .set(DATETIME, dateTime.substring(0, dateTime.length() - 1))
                    .set(COMPANY_NAME, c.element().getKey())
                    .set(AVERAGE_CLOSE, c.element().getValue()));
        }

        public static TableSchema getSchema() {
            return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
                {
                    add(new TableFieldSchema().setName(DATETIME).setType("DATETIME"));
                    add(new TableFieldSchema().setName(COMPANY_NAME).setType("STRING"));
                    add(new TableFieldSchema().setName(AVERAGE_CLOSE).setType("FLOAT"));

                }
            });
        }
    }

}
