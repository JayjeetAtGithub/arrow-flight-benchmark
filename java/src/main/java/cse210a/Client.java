package cse210a;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

class Client {
    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("host", true, "The host where the server runs.");
        options.addOption("port", true, "The port to listen on.");
        options.addOption("path", "The path to read the dataset from.");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args, false);
        } catch (org.apache.commons.cli.ParseException e) {
            e.printStackTrace();
        }
        final String host = cmd.getOptionValue("host", "localhost");
        final int port = Integer.parseInt(cmd.getOptionValue("port", "33005"));
        final String path = cmd.getOptionValue("path", "/Users/jayjeetchakraborty/flight_datasets");

        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        FlightDescriptor flightDescriptor = FlightDescriptor.path(path);
        Location location = Location.forGrpcInsecure(host, port);
        FlightClient flightClient = FlightClient.builder(allocator, location).build();
        System.out.println("Connected to " + flightClient.toString());

        FlightInfo flightInfo = flightClient.getInfo(flightDescriptor);
        System.out.println(flightInfo.toString());

        FlightStream flightStream = flightClient.getStream(flightInfo.getEndpoints().get(0).getTicket());
        System.out.println("Schema: " + flightStream.getSchema());
    }
}

