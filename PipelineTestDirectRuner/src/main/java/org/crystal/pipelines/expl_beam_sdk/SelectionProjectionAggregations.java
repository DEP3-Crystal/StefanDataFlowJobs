package org.crystal.pipelines.expl_beam_sdk;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class SelectionProjectionAggregations {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        Schema schema = Schema.builder()
                .addStringField("date")
                .addStringField("product")
                .addDoubleField("price")
                .addStringField("paymentType")
                .addStringField("country")
                .build();

        Row row1 = Row.withSchema(schema).addValues(
                "1/5/09 5:39", "Shoes", 120.1, "Amex", "Netherlands").build();
        Row row2 = Row.withSchema(schema).addValues(
                "2/2/09 9:16", "Jeans", 110.3, "Mastercard", "United States").build();
        Row row3 = Row.withSchema(schema).addValues(
                "3/5/09 10:08", "Pens", 10.0, "Visa", "United States").build();
        Row row4 = Row.withSchema(schema).addValues(
                "4/2/09 14:18", "Shoes", 303.0, "Visa", "United States").build();
        Row row5 = Row.withSchema(schema).addValues(
                "5/4/09 1:05", "iPhone", 1240.99, "Diners", "Ireland").build();
        Row row6 = Row.withSchema(schema).addValues(
                "6/5/09 11:37", "TV", 1503.6, "Visa", "Canada").build();

        PCollection<Row> inputTable = PBegin.in(pipeline)
                .apply(Create.of(row1, row2, row3, row4, row5, row6)
                        .withRowSchema(schema));

        PCollection<Row> outputStream = inputTable.apply(
                SqlTransform.query("select product, max(price) from PCOLLECTION as P" +
                        " group by P.product"));

        outputStream.apply(MapElements.via(new SimpleFunction<Row, Void>() {
            @Override
            public Void apply (Row input){
                System.out.println(input);
                return null;
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}
