package crystal.ex.df;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;

public class CustomBucketToPubSub {

    public interface BucketToPubSubOptions extends PipelineOptions, StreamingOptions, DataflowPipelineOptions {

        @Description("The Cloud Pub/Sub topic to read from.")
        @Validation.Required
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        BucketToPubSubOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BucketToPubSubOptions.class);

        options.setStreaming(true);
        options.setJobName("WriteonPubSub12");
        options.setProject("big-data-test-369511");
        options.setRegion("europe-west1");
//        options.setServiceAccount("dataflowse@big-data-test-369511.iam.gserviceaccount.com");
        options.setRunner(DataflowRunner.class);
        options.setGcpTempLocation("gs://d-flow-bucket1/temp");

        runPubSubToBq(options);
    }

    static void runPubSubToBq(BucketToPubSubOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("ReadAds", TextIO.read().from("gs://d-flow-bucket1/source/car_ads*"))
                .apply("WritePubSub", PubsubIO.writeStrings()
                        .to("projects/big-data-test-369511/topics/testTopic"));
        pipeline.run().waitUntilFinish();
    }

}