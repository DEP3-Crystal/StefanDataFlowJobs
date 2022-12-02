package crystal.ex.df;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class CustomPubSubToBq {

    public interface PubSubToBqOptions extends PipelineOptions, StreamingOptions, GcpOptions, DataflowPipelineOptions {

//        @Description("The Cloud Pub/Sub topic to read from.")
//        @Validation.Required
//        ValueProvider<String> getInputTopic();
//
//        void setInputTopic(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        PubSubToBqOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToBqOptions.class);

        options.setStreaming(true);
//        options.setJobName("WriteonPubSub12");
        options.setProject("big-data-test-369511");
        options.setRegion("europe-west1");
//        options.setServiceAccount("dataflowse@big-data-test-369511.iam.gserviceaccount.com");
        options.setRunner(DataflowRunner.class);
        options.setGcpTempLocation("gs://d-flow-bucket1/temp");
        options.setStreaming(true);
        options.setJobName("readFromPUb1");

        runPubSubToBq(options);
    }

    static void runPubSubToBq(PubSubToBqOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> pubSubMessages = pipeline.apply("ReadingFromPubSub", PubsubIO.readStrings()
//                .fromTopic("projects/big-data-test-369511/topics/testTopic"));
                .fromSubscription("projects/big-data-test-369511/subscriptions/subscrtestTopic"));

        pubSubMessages
//        .apply("convertToString",
//                        ParDo.of(new DoFn<PubsubMessage, String>() {
//                            @ProcessElement
//                            public void processElement(ProcessContext c) {
//                                c.output(Objects.requireNonNull(c.element()).toString());
//
//                            }
//                        }))
                .apply("ConvertToTableRow", ParDo.of(new ConvertToTableRow()))
                .apply("WriteToBigQuery",
                        BigQueryIO.writeTableRows()
                                .to("big-data-test-369511.bqds." + ConvertToTableRow.TABLE_NAME)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
//                                .withSchema(
//                                        new TableSchema()
//                                                .setFields(
//                                                        ImmutableList.of(
//                                                                new TableFieldSchema()
//                                                                        .setName("num")
//                                                                        .setType("STRING")
//                                                                        .setMode("REQUIRED"),
//                                                                new TableFieldSchema()
//                                                                        .setName(ConvertToTableRow.COMPANY)
//                                                                        .setType("STRING")
//                                                                        .setMode("REQUIRED"))))
                                .withSchema(ConvertToTableRow.getSchema())
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run().waitUntilFinish();
    }


    public static class ConvertToTableRow extends DoFn<String, TableRow> {

        public static final String CAR = "car";
        public static final String PRICE = "price";
        public static final String TABLE_NAME = "cars";

        @ProcessElement
        public void processElement(DoFn<String, TableRow>.ProcessContext c) {
            String[] input = c.element().split(",");
            c.output(new TableRow()
                    .set(CAR, input[0])
                    .set(PRICE, input[1])
            );
        }

        public static TableSchema getSchema() {
            return new TableSchema().setFields(ImmutableList.of(
                new TableFieldSchema()
                            .setName(CAR)
                            .setType("STRING")
                            .setMode("REQUIRED"),
                    new TableFieldSchema()
                            .setName(PRICE)
                            .setType("STRING")
                            .setMode("REQUIRED"))
            );
        }
    }

}
