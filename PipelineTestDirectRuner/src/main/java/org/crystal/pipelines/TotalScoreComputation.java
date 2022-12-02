package org.crystal.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

import java.util.Objects;

public class TotalScoreComputation {
    private static  final String CSV_HEADER=
            "id,Name,Physics,Chemistry,Math,English,Biology,History";
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from("src/main/resources/source/source.csv"))
                .apply(ParDo.of(new FilteredHeaderFn(CSV_HEADER)))
                .apply(ParDo.of(new ComputeTotalScoreFn()))
                .apply(ParDo.of(new ConvertToStringFn()))
        .apply(TextIO.write().withoutSharding().to("src/main/resources/sink/sink.csv").withHeader("Name,Total"));

        pipeline.run();
    }

    public static class FilteredHeaderFn extends DoFn<String,String> {
        private final String header;
        public FilteredHeaderFn(String header) {
            this.header = header;
        }
        @ProcessElement
        public void pel(ProcessContext processContext){
            String row =processContext.element();

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

    public static class ConvertToStringFn extends  DoFn<KV<String,Integer>,String> {
        @ProcessElement
        public void processElement(ProcessContext c){
            c.output(Objects.requireNonNull(c.element()).getKey()+","+ Objects.requireNonNull(c.element()).getValue());
        }


    }
}
