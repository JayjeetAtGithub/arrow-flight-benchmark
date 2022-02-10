package cse210a;


import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.JsonFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Validator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * A Flight client for integration testing.
 */
class IntegrationTestClient {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(IntegrationTestClient.class);
    private final Options options;

    private IntegrationTestClient() {
        options = new Options();
        options.addOption("j", "json", true, "json file");
        options.addOption("scenario", true, "The integration test scenario.");
        options.addOption("host", true, "The host to connect to.");
        options.addOption("port", true, "The port to connect to.");
    }

    public static void main(String[] args) {
        try {
            new IntegrationTestClient().run(args);
        } catch (ParseException e) {
            fatalError("Invalid parameters", e);
        } catch (IOException e) {
            fatalError("Error accessing files", e);
        } catch (Exception e) {
            fatalError("Unknown error", e);
        }
    }

    private static void fatalError(String message, Throwable e) {
        System.err.println(message);
        System.err.println(e.getMessage());
        LOGGER.error(message, e);
        System.exit(1);
    }

    private void run(String[] args) throws Exception {
        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd = parser.parse(options, args, false);

        final String host = cmd.getOptionValue("host", "localhost");
        final int port = Integer.parseInt(cmd.getOptionValue("port", "31337"));

        final Location defaultLocation = Location.forGrpcInsecure(host, port);
        try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
             final FlightClient client = FlightClient.builder(allocator, defaultLocation).build()) {
                final String inputPath = cmd.getOptionValue("j");
                testStream(allocator, defaultLocation, client, inputPath);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void testStream(BufferAllocator allocator, Location server, FlightClient client, String inputPath)
            throws IOException {
        FlightDescriptor descriptor = FlightDescriptor.path(inputPath);

        // 2. Get the ticket for the data.
        FlightInfo info = client.getInfo(descriptor);
        List<FlightEndpoint> endpoints = info.getEndpoints();
        if (endpoints.isEmpty()) {
            throw new RuntimeException("No endpoints returned from Flight server.");
        }

        for (FlightEndpoint endpoint : info.getEndpoints()) {
            // 3. Download the data from the server.
            List<Location> locations = endpoint.getLocations();
            if (locations.isEmpty()) {
                throw new RuntimeException("No locations returned from Flight server.");
            }
            for (Location location : locations) {
                System.out.println("Verifying location " + location.getUri());
                try (FlightClient readClient = FlightClient.builder(allocator, location).build();
                     FlightStream stream = readClient.getStream(endpoint.getTicket());
                     VectorSchemaRoot root = stream.getRoot();
                     VectorSchemaRoot downloadedRoot = VectorSchemaRoot.create(root.getSchema(), allocator);
                     JsonFileReader reader = new JsonFileReader(new File(inputPath), allocator)) {
                    VectorLoader loader = new VectorLoader(downloadedRoot);
                    VectorUnloader unloader = new VectorUnloader(root);

                    Schema jsonSchema = reader.start();
                    Validator.compareSchemas(root.getSchema(), jsonSchema);
                    try (VectorSchemaRoot jsonRoot = VectorSchemaRoot.create(jsonSchema, allocator)) {

                        while (stream.next()) {
                            try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
                                loader.load(arb);
                                if (reader.read(jsonRoot)) {

                                    // 4. Validate the data.
                                    Validator.compareVectorSchemaRoot(jsonRoot, downloadedRoot);
                                    jsonRoot.clear();
                                } else {
                                    throw new RuntimeException("Flight stream has more batches than JSON");
                                }
                            }
                        }

                        // Verify no more batches with data in JSON
                        // NOTE: Currently the C++ Flight server skips empty batches at end of the stream
                        if (reader.read(jsonRoot) && jsonRoot.getRowCount() > 0) {
                            throw new RuntimeException("JSON has more batches with than Flight stream");
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
