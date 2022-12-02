package crystal.ex;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.net.URL;
import java.util.stream.Collectors;

public class SQLCountExample {


    final static String HEADER = "userId,orderId,productId,Amount";

    final static Schema schema = Schema.builder().addStringField("userId")
            .addStringField("orderId")
            .addStringField("productId")
            .addDoubleField("Amount").build();

    public static void main(String[] args) {


        PipelineOptions opts = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(opts);
//		Step 1 : Read csv file.
        System.out.println(opts.getRunner());
        PCollection<String> fileInput = pipeline.apply(TextIO.read()
                .from("src\\main\\resources\\src\\020 user-order.csv"));

//		Step 2 : Convert PCollection<String> to PCollection<Row>


        PCollection<Row> rowInput = fileInput.apply(ParDo.of(new StringToRow())).setRowSchema(schema);

//		Step 3 : Apply SqlTransform.query

        PCollection<Row> sqlInput = rowInput
                .apply(
                        SqlTransform.query("select userId,Count(userId) from PCOLLECTION group by userId "));

//		Step 4 : Convert PCollection<Row> to PCollection<String>

        PCollection<String> pOutput = sqlInput.apply(ParDo.of(new RowToString()));

        pOutput.apply(TextIO.write().to("src\\main\\resources\\sink\\output").withNumShards(1).withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }


    //ParDo for String -> Row (SQL)
    public static class StringToRow extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {

            if (!c.element().equalsIgnoreCase(HEADER)) {
                String arr[] = c.element().split(",");

                Row record = Row.withSchema(schema).addValues(arr[0], arr[1], arr[2], Double.valueOf(arr[3])).build();
                c.output(record);
            }
        }
    }

    //ParDo for Row (SQL) -> String
    public static class RowToString extends DoFn<Row, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {

            String outString = c.element().getValues().stream().
                    map(Object::toString).collect(Collectors.joining(","));

            c.output(outString);
        }
    }

}
