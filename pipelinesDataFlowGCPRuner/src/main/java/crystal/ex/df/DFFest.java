package crystal.ex.df;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
//done
public class DFFest {

    public static void main(String[] args) {

        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
        options.setProject("big-data-test-369511");
        options.setRegion("europe-west1");
//        options.setServiceAccount("dataflowse@big-data-test-369511.iam.gserviceaccount.com");
        options.setJobName("testing-pipeline1");
        options.setRunner(DataflowRunner.class);
        options.setGcpTempLocation("gs://d-flow-bucket1/temp");
        Pipeline pipeline =Pipeline.create(options);

        PCollection<String> in;
//            in=pipeline.apply("Read file", Create.of(Files.readLines(new File("gs://d-flow-bucket/source/p-user.csv"), StandardCharsets.ISO_8859_1)));
            in=pipeline.apply("Read file", TextIO.read().from("gs://d-flow-bucket1/source/SalesJan2009.csv"));

        in.apply("write on bucket", TextIO.write().to("gs://d-flow-bucket1/sink/p-user.csv").withoutSharding());
pipeline.run().waitUntilFinish();
    }
}
