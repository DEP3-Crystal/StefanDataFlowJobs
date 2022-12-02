package org.crystal.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

public class FilterNumbers {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        List<Double> list = Arrays.asList(1555.5,1220.5,1394.2,1404.6);
        PCollection<Double> print_not_filtered = pipeline.apply(Create.of(list))
                .apply("print not filtered", MapElements.via(new SimpleFunction<Double, Double>() {
                    @Override
                    public Double apply(Double input) {
                        System.out.println("--Pre filtered : " + input);
                        return input;

                    }
                }));

        PCollection<Double> applay = print_not_filtered.apply(" filter elements", ParDo.of(new FilteredThreshold(1400)))
                .apply("print filtered",MapElements.via(new SimpleFunction<Double, Double>() {
                    @Override
                    public Double apply(Double input){
                        System.out.println("--Post filtered : "+input);
                        return input;
                    }
                }));


        pipeline.run();
    }
}
