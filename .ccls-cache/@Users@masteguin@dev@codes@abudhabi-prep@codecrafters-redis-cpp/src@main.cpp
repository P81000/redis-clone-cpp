#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <vector>
#include <unordered_map>
#include <mutex>

struct CacheEntry {
  std::string value;
  std::chrono::time_point<std::chrono::system_clock> exp_time;
  bool has_exp = false;
};

struct ServerState {
  std::mutex rw_mtx;
  std::unordered_map<std::string, CacheEntry> db;
};

std::vector<std::string> parse_resp(const std::string& input) {
  std::vector<std::string> tokens;
  size_t start = 0;
  size_t end = input.find("\r\n");

  while (end != std::string::npos) {
    tokens.push_back(input.substr(start, end - start));
    start = end + 2;
    end = input.find("\r\n", start);
  }

  return tokens;
}

void handle_client(int client_fd, ServerState& state) {
  char buffer[1024];
  std::string response;

  for (;;) {
    int bytes = recv(client_fd, buffer, sizeof(buffer), 0);
    if (bytes <= 0) {
      std::cout << "Client disconnected.\n";
      break;
    }
    
    std::string request(buffer, bytes);
    std::vector<std::string> tokens = parse_resp(request);

    if (tokens.size() >= 3) {
      std::string cmd = tokens[2];

      for (auto& c : cmd) c = toupper(c);

      if (cmd == "PING") {
        response = "+PONG\r\n";
      } else if (cmd == "ECHO" && tokens.size() >= 5) {
        std::string arg = tokens[4];

        response = "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
      } else if (cmd == "SET" && tokens.size() >= 3){
        std::string var_name = tokens[4];
        std::string var_value = tokens[6];

        CacheEntry entry;
        entry.value = var_value;
        
//                    SET          mykey         value         PX        1000
// arr_size cmd_size      op_size        op_size       op_size    op_size

        if (tokens.size() >= 11) {
          std::string op = tokens[8];
          for (auto& c : op) c = toupper(c);

          int duration = std::stoi(tokens[10]);

          if (op == "EX") {
            entry.exp_time = std::chrono::system_clock::now() + std::chrono::seconds(duration);
            entry.has_exp = true;
          } else if (op == "PX") {
            entry.exp_time = std::chrono::system_clock::now() + std::chrono::milliseconds(duration);
            entry.has_exp = true;
          }
        }

        { // lock_guard
          std::lock_guard<std::mutex> lk(state.rw_mtx);
          state.db.emplace(var_name, var_value);
        }

        response = "+OK\r\n";
      } else if (cmd == "GET" && tokens.size() >= 2) {
        std::string look_for = tokens[4];

        {
          std::lock_guard<std::mutex> lk(state.rw_mtx);
          auto search = state.db.find(look_for);

          if (search != state.db.end()) {
            if (search->second.has_exp && std::chrono::system_clock::now() > search->second.exp_time) {
              state.db.erase(search);
              response = "$-1\r\n";
            } else {
              std::string val = search->second.value;
              response = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
            }
          } else {
            response = "$-1\r\n";
          }
        }
      } else { // default answ
        response = "-ERR unknown command\r\n";
      }
    }

    send(client_fd, response.c_str(), response.length(), 0);
  }

  close(client_fd);
}

int main(void) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  ServerState shared_state;
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  for (;;) {
    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);
    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);

    if (client_fd < 0) {
      std::cerr << "Accept failed\n";
      continue;
    }

    std::cout << "New client connected..\n";

    std::thread client_thread(handle_client, client_fd, std::ref(shared_state));

    client_thread.detach();
  }
  
  close(server_fd);
  return 0;
}
