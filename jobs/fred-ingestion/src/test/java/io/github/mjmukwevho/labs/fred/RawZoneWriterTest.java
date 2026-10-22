package io.github.mjmukwevho.labs.fred;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RawZoneWriterTest {

    @TempDir
    Path rawDir;

    private static final Instant INGESTED_AT = Instant.parse("2026-10-22T06:30:00Z");

    /** Records writes in memory, in order. */
    static class InMemoryStorage implements RawStorage {
        final Map<String, String> objects = new LinkedHashMap<>();
        final List<String> order = new ArrayList<>();

        @Override
        public String put(String relativePath, String content) {
            objects.put(relativePath, content);
            order.add(relativePath);
            return "mem://" + relativePath;
        }
    }

    @Test
    void writesHivePartitionedDataThenMetadata() throws Exception {
        InMemoryStorage storage = new InMemoryStorage();

        String location = new RawZoneWriter(storage)
                .write("SP500", "{\"observations\":[]}", Optional.of(LocalDate.of(2026, 1, 1)), INGESTED_AT);

        String partition = "fred/series_id=SP500/ingest_date=2026-10-22";
        assertEquals("mem://" + partition + "/observations.json", location);
        assertEquals("{\"observations\":[]}", storage.objects.get(partition + "/observations.json"));
        // Metadata last, so its presence marks a complete partition
        assertEquals(List.of(partition + "/observations.json", partition + "/_metadata.json"), storage.order);

        String metadata = storage.objects.get(partition + "/_metadata.json");
        assertTrue(metadata.contains("\"series_id\": \"SP500\""), metadata);
        assertTrue(metadata.contains("\"observation_start\": \"2026-01-01\""), metadata);
        assertTrue(metadata.contains("\"ingested_at\": \"2026-10-22T06:30:00Z\""), metadata);
    }

    @Test
    void localStorageWritesFilesAndOverwritesOnRerun() throws Exception {
        RawZoneWriter writer = new RawZoneWriter(new LocalRawStorage(rawDir));

        writer.write("DGS10", "first", Optional.empty(), INGESTED_AT);
        String location = writer.write("DGS10", "second", Optional.empty(), INGESTED_AT);

        Path file = rawDir.resolve("fred/series_id=DGS10/ingest_date=2026-10-22/observations.json");
        assertEquals(file.toString(), location);
        assertEquals("second", Files.readString(file));
        assertFalse(Files.exists(file.resolveSibling("observations.json.tmp")));

        String metadata = Files.readString(file.resolveSibling("_metadata.json"));
        assertTrue(metadata.contains("\"observation_start\": null"), metadata);
    }
}
