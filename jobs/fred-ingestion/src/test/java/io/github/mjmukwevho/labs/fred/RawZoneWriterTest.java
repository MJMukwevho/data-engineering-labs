package io.github.mjmukwevho.labs.fred;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RawZoneWriterTest {

    @TempDir
    Path rawDir;

    private static final Instant INGESTED_AT = Instant.parse("2026-10-22T06:30:00Z");

    @Test
    void writesHivePartitionedFileAndMetadata() throws Exception {
        RawZoneWriter writer = new RawZoneWriter(rawDir);

        Path file = writer.write("SP500", "{\"observations\":[]}", Optional.of(LocalDate.of(2026, 1, 1)), INGESTED_AT);

        Path partition = rawDir.resolve("fred/series_id=SP500/ingest_date=2026-10-22");
        assertEquals(partition.resolve("observations.json"), file);
        assertEquals("{\"observations\":[]}", Files.readString(file));

        String metadata = Files.readString(partition.resolve("_metadata.json"));
        assertTrue(metadata.contains("\"series_id\": \"SP500\""), metadata);
        assertTrue(metadata.contains("\"observation_start\": \"2026-01-01\""), metadata);
        assertTrue(metadata.contains("\"ingested_at\": \"2026-10-22T06:30:00Z\""), metadata);
        assertFalse(Files.exists(partition.resolve("observations.json.tmp")));
    }

    @Test
    void rerunOnSameDayOverwritesPartition() throws Exception {
        RawZoneWriter writer = new RawZoneWriter(rawDir);

        writer.write("DGS10", "first", Optional.empty(), INGESTED_AT);
        Path file = writer.write("DGS10", "second", Optional.empty(), INGESTED_AT);

        assertEquals("second", Files.readString(file));
        String metadata = Files.readString(file.resolveSibling("_metadata.json"));
        assertTrue(metadata.contains("\"observation_start\": null"), metadata);
    }
}
