import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

import java.io.Serializable;
import java.util.Objects;

@DefaultSchema(JavaBeanSchema.class)
public class StudentScore implements Serializable {

    private int studentId;
    private String subject;
    private int score;

    public StudentScore() {
    }

    @SchemaCreate
    public StudentScore(Integer studentId, String subject, Integer score) {
        this.studentId = studentId;
        this.subject = subject;
        this.score = score;
    }

    public Integer getStudentId() {
        return studentId;
    }

    public void setStudentId(Integer studentId) {
        this.studentId = studentId;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public Integer getScore() { return score; }

    public void setScore(Integer score) {
        this.score = score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StudentScore studentScore = (StudentScore) o;
        return studentId == studentScore.studentId &&
               subject.equals(studentScore.subject) &&
               score == studentScore.score;
    }

    @Override
    public int hashCode() {
        return Objects.hash(studentId, subject, score);
    }
}
