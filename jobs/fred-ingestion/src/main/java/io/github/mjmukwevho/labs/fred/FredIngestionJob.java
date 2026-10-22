package io.github.mjmukwevho.labs.fred;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry point: pulls each configured FRED series and lands it in the raw layer.
 * One failing series does not stop the others; the job exits non-zero if any failed.
 */
public class FredIngestionJob {

    public static void main(String[] args) {
        IngestionConfig config;
        try {
            config = IngestionConfig.fromEnvironment(System.getenv());
        } catch (IllegalArgumentException e) {
            System.err.println("Configuration error: " + e.getMessage());
            System.exit(2);
            return;
        }

        int failures = run(config, new FredClient(config.apiKey()), new RawZoneWriter(config.rawDataDir()));
        System.exit(failures == 0 ? 0 : 1);
    }

    /** Returns the number of series that failed. */
    static int run(IngestionConfig config, FredClient client, RawZoneWriter writer) {
        Instant ingestedAt = Instant.now();
        List<String> failed = new ArrayList<>();

        for (String seriesId : config.seriesIds()) {
            try {
                String body = client.fetchObservations(seriesId, config.observationStart());
                var path = writer.write(seriesId, body, config.observationStart(), ingestedAt);
                System.out.println("OK    " + seriesId + " -> " + path);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failed.add(seriesId);
                break;
            } catch (Exception e) {
                // Some network errors have no message, so fall back to the exception type
                String reason = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                System.err.println("FAIL  " + seriesId + ": " + reason);
                failed.add(seriesId);
            }
        }

        System.out.printf("Done: %d succeeded, %d failed%s%n",
                config.seriesIds().size() - failed.size(),
                failed.size(),
                failed.isEmpty() ? "" : " " + failed);
        return failed.size();
    }
}
