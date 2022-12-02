import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

import java.io.Serializable;
import java.util.Objects;

@DefaultSchema(JavaBeanSchema.class)
public class Student implements Serializable {

    private int id;
    private int year;
    private String name;
    private String department;
    private StudentAddress address;

    public Student() {}

    @SchemaCreate
    public Student(Integer id, Integer year, String name,
                   String department, StudentAddress address) {
        this.id = id;
        this.year = year;
        this.name = name;
        this.department = department;
        this.address = address;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public void setAddress(StudentAddress address) {
        this.address = address;
    }

    public StudentAddress getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Student student = (Student) o;

        return id == student.id &&
                year == student.year &&
                name.equals(student.name) &&
                department.equals(student.department) &&
                address.equals(student.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, year, name, department, address);
    }
}
