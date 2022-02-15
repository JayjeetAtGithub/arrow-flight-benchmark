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


import org.apache.arrow.flight.*;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

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




}