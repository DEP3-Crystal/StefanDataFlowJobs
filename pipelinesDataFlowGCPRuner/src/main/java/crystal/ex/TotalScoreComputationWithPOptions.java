package crystal.ex;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.text.DecimalFormat;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TotalScoreComputationWithPOptions {
    private static  final String CSV_HEADER=
            "id,Name,Physics,Chemistry,Math,English,Biology,History";
    public interface TotalScoreComputationPOptions extends PipelineOptions{
        @Description("Path of the file to read from")
        @Default.String("src/main/resources/source/source.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.String("src/main/resources/sink/sink.csv")

        String getOutputFile();

        void setOutputFile(String value);
    }
    public static void main(String[] args) {
        TotalScoreComputationPOptions options = PipelineOptionsFactory.fromArgs(args)
                                                                        .withValidation()
                                                                        .as(TotalScoreComputationPOptions.class);

        Pipeline pipeline = Pipeline.create(options);
options.setJobName("Compute some stuff for student");
        pipeline.apply(TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new FilteredHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeTotalScoreFn()))
                .apply(ParDo.of(new ConvertToStringFn()))
                .apply(ParDo.of(new ComputeAverageScoreFn()))
                .apply(TextIO.write().withoutSharding()
                        .to(options.getOutputFile())
                        .withHeader("Name,Total,Average") );

        pipeline.run().waitUntilFinish();
        System.out.println(options.getJobName());

        ExecutorService executorService= Executors.newFixedThreadPool(10);

    }

    public static class FilteredHeaderFn extends DoFn<String,String> {
        private final String header;
        public FilteredHeaderFn(String header) {
            this.header = header;
        }
        @ProcessElement
        public void pel(ProcessContext processContext){
            String row =processContext.element();

            assert row != null;
            if (!row.isEmpty()&&!row.equals(this.header)){
                processContext.output(row);
            }

        }
    }

    public static class ComputeTotalScoreFn extends DoFn<String, KV<String,Integer>> {

        @ProcessElement
        public void pel(ProcessContext c){
       String[] data= Objects.requireNonNull(c.element()).split(",");
       String name=data[1];
       Integer totalScore=
               Integer.parseInt(data[2])+
               Integer.parseInt(data[3])+
               Integer.parseInt(data[4])+
               Integer.parseInt(data[5])+
               Integer.parseInt(data[6])+
               Integer.parseInt(data[7]);
       c.output(KV.of(name,totalScore));
        }
    }
    public static class ComputeAverageScoreFn extends DoFn<String,String> {

        @ProcessElement
        public void processElement(ProcessContext c){
       String[] data= Objects.requireNonNull(c.element()).split(",");
       String name=data[0];
       double score= Double.parseDouble(data[1]);

       c.output(name+","+score+","+new DecimalFormat("##.00").format(score/6));
        }
    }

    public static class ConvertToStringFn extends  DoFn<KV<String,Integer>,String> {
        @ProcessElement
        public void processElement(ProcessContext c){
            c.output(Objects.requireNonNull(c.element()).getKey()+","+ Objects.requireNonNull(c.element()).getValue());
        }


    }
}
