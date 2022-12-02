package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;

import java.util.ArrayList;

public class CustomPubSubToBq {

    public interface PubSubToBqOptions extends PipelineOptions, StreamingOptions {
        
        @Description("The Cloud Pub/Sub topic to read from.")
        @Validation.Required
        ValueProvider<String> getInputTopic();
        void setInputTopic(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        PubSubToBqOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation().as(PubSubToBqOptions.class);
        
        options.setStreaming(true);
    
        runPubSubToBq(options);
    }

    static void runPubSubToBq(PubSubToBqOptions options) {
        
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> pubSubMessages = pipeline.apply("ReadingFromPubSub", PubsubIO.readStrings()
            .fromTopic(options.getInputTopic()));

        pubSubMessages.apply("ConvertToTableRow", ParDo.of(new ConvertToTableRow()))
                .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                    .to("loony-dataflow-review:results." + ConvertToTableRow.TABLE_NAME)
                        .withSchema(ConvertToTableRow.getSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run();
    }


    public static class ConvertToTableRow extends DoFn<String, TableRow> {

        public static final String COMPANY = "company";
        public static final String CLOSE = "close";
        public static final String TABLE_NAME = "stockClosePrices";

        @ProcessElement
        public void processElement(DoFn<String, TableRow>.ProcessContext c) {
            String input = c.element();

            JSONObject obj = new JSONObject(input);

            c.output(new TableRow()
                    .set(COMPANY, obj.getString(COMPANY))
                    .set(CLOSE, obj.getDouble(CLOSE))
            );
        }

        public static TableSchema getSchema() {
            return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
                {
                    add(new TableFieldSchema().setName(COMPANY).setType("STRING").setMode("REQUIRED"));
                    add(new TableFieldSchema().setName(CLOSE).setType("FLOAT").setMode("NULLABLE"));
                }
            });
        }
    }

}
