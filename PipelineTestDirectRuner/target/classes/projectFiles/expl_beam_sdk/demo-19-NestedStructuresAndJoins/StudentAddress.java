import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

import java.io.Serializable;
import java.util.Objects;

@DefaultSchema(JavaBeanSchema.class)
public class StudentAddress implements Serializable {

    private String street;
    private int postalCode;

    public StudentAddress() {}

    @SchemaCreate
    public StudentAddress(String street, int postalCode) {
        this.street = street;
        this.postalCode = postalCode;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public Integer getPostalCode() {
        return postalCode;
    }

    public void setPostal(Integer postalCode) {
        this.postalCode = postalCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StudentAddress address = (StudentAddress) o;
        return postalCode == address.postalCode &&
                street.equals(address.street);
    }

    @Override
    public int hashCode() {
        return Objects.hash(street, postalCode);
    }
}