package cse210a;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class ParquetStorageService {
    private final String host;
    private final int port;
    private final FlightProducer flightProducer;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public FlightProducer getFlightProducer() {
        return flightProducer;
    }

    public static Schema createDefaultSchema() {
        Field strField = new Field("col1", FieldType.nullable(new ArrowType.Utf8()), null);
        Field intField = new Field("col2", FieldType.nullable(new ArrowType.Int(32, true)), null);
        List<Field> fields = new ArrayList<Field>();
        fields.add(strField);
        fields.add(intField);
        return new Schema(fields);
    }

    ParquetStorageService(String host, int port) {
        this.host = host;
        this.port = port;
        this.flightProducer = new FlightProducer() {
            @Override
            public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
                BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                FileSystemDatasetFactory factory = new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, "file:///mnt/data/flight_dataset/16MB.uncompressed.parquet.1");
                Dataset dataset = factory.finish();
                Schema schema = factory.inspect();
                ScanOptions options = new ScanOptions(1024 * 1024);

                List<ArrowRecordBatch> resultBatches = new ArrayList<>();
                List<ArrowRecordBatch> arrowRecordBatches = null;

                Scanner scanner = null;
                for (int i = 0; i < 100; i++) {
                    scanner = dataset.newScan(options);
                    arrowRecordBatches = stream(scanner.scan())
                            .flatMap(t -> stream(t.execute()))
                            .collect(Collectors.toList());
                    resultBatches.addAll(arrowRecordBatches);
                }

                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    DictionaryProvider dictionaryProvider = new DictionaryProvider() {
                        @Override
                        public Dictionary lookup(long l) {
                            return null;
                        }
                    };
                    serverStreamListener.start(root, dictionaryProvider);
                    final VectorLoader loader = new VectorLoader(root);
                    int counter = 0;
                    for (ArrowRecordBatch batch : resultBatches) {
                        final byte[] rawMetadata = Integer.toString(counter).getBytes(StandardCharsets.UTF_8);
                        final ArrowBuf metadata = allocator.buffer(rawMetadata.length);
                        metadata.writeBytes(rawMetadata);
                        loader.load(batch);
                        serverStreamListener.putNext(metadata);
                        counter++;
                    }
                    serverStreamListener.completed();
                } catch (Exception ex) {
                    serverStreamListener.error(ex);
                }

                try {
                    AutoCloseables.close(arrowRecordBatches);
                    AutoCloseables.close(allocator, factory, scanner, dataset);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {

            }

            @Override
            public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
                Schema schema = createDefaultSchema();
                String path = "file://" + flightDescriptor.getPath().get(0);
                Ticket ticket = new Ticket(path.getBytes(StandardCharsets.UTF_8));
                Location location = Location.forGrpcInsecure("localhost", port);
                List<Location> locations = new ArrayList<Location>();
                FlightEndpoint endpoint = new FlightEndpoint(ticket, location);
                List<FlightEndpoint> endpoints = new ArrayList<FlightEndpoint>();
                endpoints.add(endpoint);
                locations.add(location);
                FlightDescriptor descriptor = FlightDescriptor.path(path);
                System.out.println("Schema: " + schema);
                return new FlightInfo(schema, descriptor, endpoints, 0, 0);
            }

            @Override
            public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
                return null;
            }

            @Override
            public void doAction(CallContext context, Action action, StreamListener<Result> listener) {

            }

            @Override
            public void listActions(CallContext context, StreamListener<ActionType> listener) {

            }
        };
    }

    protected <T> Stream<T> stream(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    protected <T> List<T> collect(Iterable<T> iterable) {
        return stream(iterable).collect(Collectors.toList());
    }

    protected <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

    protected <T> List<T> collect(Iterator<T> iterator) {
        return stream(iterator).collect(Collectors.toList());
    }
}

public class Server {
    public static void main(String args[]) {
        Options options = new Options();
        Option hostConfig = Option.builder("h").longOpt("host")
                .argName("host")
                .hasArg()
                .required(true)
                .desc("The hostname to listen on").build();
        options.addOption(hostConfig);
        Option portConfig = Option.builder("p").longOpt("port")
                .argName("port")
                .hasArg()
                .required(true)
                .desc("The port to listen on").build();
        options.addOption(portConfig);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        HelpFormatter helper = new HelpFormatter();

        try {
            cmd = parser.parse(options, args);
        } catch (org.apache.commons.cli.ParseException e) {
            helper.printHelp("Usage: ", options);
            System.exit(0);
        }
        final String host = cmd.getOptionValue("host", "localhost");
        final int port = Integer.parseInt(cmd.getOptionValue("port", "33005"));

        ParquetStorageService parquetStorageService = new ParquetStorageService(host, port);

        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Location location = Location.forGrpcInsecure(host,port);
        FlightServer flightServer = FlightServer.builder(allocator, location, parquetStorageService.getFlightProducer()).build();
        try {
            flightServer.start();
            System.out.println("Server listening on localhost:" + flightServer.getPort());
        } catch (IOException e) {
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("\nExiting...");
                AutoCloseables.close(flightServer, allocator);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        try {
            flightServer.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}