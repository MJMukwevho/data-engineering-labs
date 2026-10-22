package io.github.mjmukwevho.labs.fred;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.mjmukwevho.labs.fred.S3RawStorage.S3Location;
import org.junit.jupiter.api.Test;

class S3LocationTest {

    @Test
    void parsesBucketAndPrefix() {
        assertEquals(new S3Location("lake", "raw"), S3Location.parse("s3://lake/raw"));
        assertEquals(new S3Location("lake", "raw/v1"), S3Location.parse("s3://lake/raw/v1/"));
        assertEquals(new S3Location("lake", ""), S3Location.parse("s3://lake"));
        assertEquals(new S3Location("lake", ""), S3Location.parse("s3://lake/"));
    }

    @Test
    void rejectsInvalidLocations() {
        assertThrows(IllegalArgumentException.class, () -> S3Location.parse("s3://"));
        assertThrows(IllegalArgumentException.class, () -> S3Location.parse("/data/raw"));
    }
}
