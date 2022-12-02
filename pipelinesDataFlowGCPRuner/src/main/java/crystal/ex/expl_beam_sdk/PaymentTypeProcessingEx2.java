package crystal.ex.expl_beam_sdk;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PaymentTypeProcessingEx2 {

    public static void main(String[] args) {

        final List<String> LINES = Arrays.asList(
                "1/5/09 5:39,Shoes,1200,Amex,Netherlands",
                "1/2/09 9:16,Jacket,1200,Mastercard,United States",
                "1/5/09 10:08,Phone,3600,Visa,United States",
                "1/2/09 14:18,Shoes,1200,Visa,United States",
                "1/4/09 1:05,Phone,3600,Diners,Ireland",
                "1/5/09 11:37,Books,1200,Visa,Canada");

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
                .apply("PrintInput", MapElements.via(new SimpleFunction<String, String>() {

                    @Override
                    public String apply(String input) {
                        System.out.println(input);
                        return input;
                    }

                }))
                .apply("ExtractPaymentTypes", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> Collections.singletonList(line.split(",")[3])))
                .apply("PrintExtractedOutput", MapElements.via(new SimpleFunction<String, Void>() {

                    @Override
                    public Void apply(String input) {
                        System.out.println(input);
                        return null;
                    }

                }));

        pipeline.run().waitUntilFinish();

        System.out.println("Pipeline execution complete!");
    }
}
