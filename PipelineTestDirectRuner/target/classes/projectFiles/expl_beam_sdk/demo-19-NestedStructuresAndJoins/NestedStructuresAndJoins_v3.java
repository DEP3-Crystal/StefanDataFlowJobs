import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class NestedStructuresAndJoins {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<Student> inputTable = pipeline.apply(
                Create.of(
                        new Student(4126, 2018,
                                "Alice", "Chemistry",
                                new StudentAddress("Wall Street", 10005)),
                        new Student(4127, 2019,
                                "Bob", "Economics",
                                new StudentAddress("Broadway", 10001)),
                        new Student(5080, 2018,
                                "Charles", "Computer Science",
                                new StudentAddress("Bourbon Street", 70130)),
                        new Student(5089, 2019,
                                "James", "Computer Science",
                                new StudentAddress("Broadway", 10001)),
                        new Student(3116, 2018,
                                "Julie", "English",
                                new StudentAddress("Broadway", 10001)),
                        new Student(3119, 2019,
                                "Ronda", "Math",
                                new StudentAddress("Wall Street", 10005))
                ));

        PCollection<Row> outputStream = inputTable.apply(
                SqlTransform.query("select id, name, department, address "
                        + "from PCOLLECTION as P "
                        + "where P.address.street = 'Broadway'"));

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
