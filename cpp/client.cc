#include <iostream>
#include <time.h>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

class MeasureExecutionTime{
  private:
      const std::chrono::steady_clock::time_point begin;
      const std::string caller;
  public:
      MeasureExecutionTime(const std::string& caller):caller(caller),begin(std::chrono::steady_clock::now()){}
      ~MeasureExecutionTime(){
          const auto duration=std::chrono::steady_clock::now()-begin;
          std::cout << (double)std::chrono::duration_cast<std::chrono::milliseconds>(duration).count()/1000<<std::endl;
      }
};

#ifndef MEASURE_FUNCTION_EXECUTION_TIME
#define MEASURE_FUNCTION_EXECUTION_TIME const MeasureExecutionTime measureExecutionTime(__FUNCTION__);
#endif

struct ConnectionInfo {
  std::string host;
  int32_t port;
};

arrow::Result<std::unique_ptr<arrow::flight::FlightClient>> ConnectToFlightServer(ConnectionInfo info) {
  arrow::flight::Location location;
  ARROW_RETURN_NOT_OK(
      arrow::flight::Location::ForGrpcTcp(info.host, info.port, &location));

  std::unique_ptr<arrow::flight::FlightClient> client;
  ARROW_RETURN_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));
  //std::cout << "Connected to " << location.ToString() << std::endl;
  return client;
}

int main(int argc, char *argv[]) {
  // Get connection info from user input
  ConnectionInfo info;
  info.host = argv[1];
  info.port = (int32_t)std::stoi(argv[2]);

   // Connect to flight server
  auto client = ConnectToFlightServer(info).ValueOrDie();
  auto descriptor = arrow::flight::FlightDescriptor::Path({argv[3]});

  // Get flight info
  std::unique_ptr<arrow::flight::FlightInfo> flight_info;
  client->GetFlightInfo(descriptor, &flight_info);
  //std::cout << flight_info->descriptor().ToString() << std::endl;

  // Read table from flight server
  std::shared_ptr<arrow::Table> table;
  for(int i=0;i<10;i++){
    {
      MEASURE_FUNCTION_EXECUTION_TIME
      std::unique_ptr<arrow::flight::FlightStreamReader> stream;
      client->DoGet(flight_info->endpoints()[0].ticket, &stream);
      stream->ReadAll(&table);
    }
  }
  //std::cout << table->ToString() << std::endl;
}
