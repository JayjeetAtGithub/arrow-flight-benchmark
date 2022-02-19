package cse210a;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.cli.*;

class Client {
    public static void main(String[] args) {
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
        Option fileConfig = Option.builder("f").longOpt("file")
                .argName("file")
                .hasArg()
                .required(true)
                .desc("The dataset path to read from").build();
        options.addOption(fileConfig);

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
        final String path = cmd.getOptionValue("path", "/mnt/data/flight_dataset");

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

