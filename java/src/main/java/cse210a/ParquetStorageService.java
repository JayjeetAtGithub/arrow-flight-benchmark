package cse210a;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


//
///**
// * Flight server for integration testing.
// */
//class IntegrationTestServer {
//	private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(IntegrationTestServer.class);
//	private final Options options;
//
//	private IntegrationTestServer() {
//		options = new Options();
//		options.addOption("port", true, "The port to serve on.");
//		options.addOption("scenario", true, "The integration test scenario.");
//	}
//
//	private void run(String[] args) throws Exception {
//		CommandLineParser parser = new DefaultParser();
//		CommandLine cmd = parser.parse(options, args, false);
//		final int port = Integer.parseInt(cmd.getOptionValue("port", "31337"));
//		final Location location = Location.forGrpcInsecure("localhost", port);
//
//		final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
//		final FlightServer.Builder builder = FlightServer.builder().allocator(allocator).location(location);
//
//		final FlightServer server;
//
//			final InMemoryStore store = new InMemoryStore(allocator, location);
//			server = FlightServer.builder(allocator, location, store).build().start();
//			store.setLocation(Location.forGrpcInsecure("localhost", server.getPort()));
//
//		// Print out message for integration test script
//		System.out.println("Server listening on localhost:" + server.getPort());
//
//		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//			try {
//				System.out.println("\nExiting...");
//				AutoCloseables.close(server, allocator);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}));
//
//		server.awaitTermination();
//	}
//
//	public static void main(String[] args) {
//		try {
//			new IntegrationTestServer().run(args);
//		} catch (ParseException e) {
//			fatalError("Error parsing arguments", e);
//		} catch (Exception e) {
//			fatalError("Runtime error", e);
//		}
//	}
//
//	private static void fatalError(String message, Throwable e) {
//		System.err.println(message);
//		System.err.println(e.getMessage());
//		LOGGER.error(message, e);
//		System.exit(1);
//	}
//
//}


import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.StreamSupport.stream;

class ParquetStorageService{

    public static void main(String[] args) {
		try {
			new ParquetStorageService().run(args);
		} catch (ParseException e) {
			System.out.println("Error passing argument");
		} catch (Exception e) {
			System.out.println("Runtime error"+ e);
		}
	}

    private void run(String[] args) throws Exception {
        FlightProducer producer= new FlightProducer() {
            @Override
            public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
//                String path=;
                BufferAllocator allocator=new RootAllocator(Long.MAX_VALUE);
                FileSystemDatasetFactory factory=new FileSystemDatasetFactory(allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET,ticket.getBytes().toString());
                Dataset dataset= factory.finish();
                ScanOptions options= new ScanOptions(1024*1024);
                Scanner scanner= dataset.newScan(options);
                final List<ArrowRecordBatch> ret = stream(scanner.scan())
                        .flatMap(t -> stream(t.execute()))
                        .collect(Collectors.toList());
                try {
                    AutoCloseables.close(scanner, dataset);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }


                try (VectorSchemaRoot root = VectorSchemaRoot.create(scanner.schema(), allocator)) {
                    DictionaryProvider dictionaryProvider=new DictionaryProvider() {
                        @Override
                        public Dictionary lookup(long l) {
                            return null;
                        }
                    };
                    serverStreamListener.start(root, dictionaryProvider);
                    final VectorLoader loader = new VectorLoader(root);
                    int counter = 0;
                    for (ArrowRecordBatch batch : ret) {
                        final byte[] rawMetadata = Integer.toString(counter).getBytes(StandardCharsets.UTF_8);
                        final ArrowBuf metadata = allocator.buffer(rawMetadata.length);
                        metadata.writeBytes(rawMetadata);
                        loader.load(batch);
                        // Transfers ownership of the buffer - do not free buffer ourselves
                        serverStreamListener.putNext(metadata);
                        counter++;
                    }
                    serverStreamListener.completed();
                } catch (Exception ex) {
                    serverStreamListener.error(ex);
                }
            }

            @Override
            public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {

            }


            @Override
            public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
//                Schema schema= new Schema();
                Schema schema= null;
                try {
                    schema = Schema.fromJSON("{}");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String path="file://"+flightDescriptor.getPath().get(0);
                Ticket ticket= new Ticket(path.getBytes(StandardCharsets.UTF_8));
                Location location= Location.forGrpcInsecure("localhost",5000);
                List<Location> locations=new ArrayList<Location>();
                FlightEndpoint endpoint=new FlightEndpoint(ticket,location);
                List<FlightEndpoint> endpoints=new ArrayList<FlightEndpoint>();
                endpoints.add(endpoint);
                locations.add(location);
                FlightDescriptor descriptor = FlightDescriptor.path(path);
                FlightInfo flightInfo= new FlightInfo(schema,descriptor,endpoints,0,0);
                return flightInfo;
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