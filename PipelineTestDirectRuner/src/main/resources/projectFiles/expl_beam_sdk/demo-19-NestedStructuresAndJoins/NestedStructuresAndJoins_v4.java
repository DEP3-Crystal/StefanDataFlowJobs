import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

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

        PCollection<StudentScore> inputScores = pipeline.apply(
                Create.of(
                        new StudentScore(4126, "Physics", 89),
                        new StudentScore(4126, "Chemistry", 78),
                        new StudentScore(4127, "Macroecomics", 80),
                        new StudentScore(4127, "Risk", 82),
                        new StudentScore(5080, "Programming", 88),
                        new StudentScore(5080, "Databases", 91),
                        new StudentScore(5089, "Programming", 92),
                        new StudentScore(5089, "Databases", 88),
                        new StudentScore(3116, "Philosophy", 96),
                        new StudentScore(3116, "Classics", 95),
                        new StudentScore(3119, "Statistics", 65),
                        new StudentScore(3119, "Finance", 89)
                ));

        PCollection<Row> outputStream = PCollectionTuple.of(new TupleTag<>("students"), inputTable)
                .and(new TupleTag<>("scores"), inputScores)
                .apply(SqlTransform.query(
                        "select students.name, department, subject, score "
                            + "from scores join students ON scores.studentId = students.id "
                            + "where students.department = 'Computer Science'"));

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
