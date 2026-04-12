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
  std::mutex mtx_var;
  std::unordered_map<std::string, CacheEntry> db_var;

  std::mutex mtx_list;
  std::unordered_map<std::string, std::vector<std::string>> db_list;
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

//     for (auto t : tokens) std::cout << "[ debug ] " << t << std::endl;

    if (tokens.size() >= 3) {
      std::string cmd = tokens[2];

      for (auto& c : cmd) c = toupper(c);

      if (cmd == "PING") {
        response = "+PONG\r\n";
      } else if (cmd == "ECHO" && tokens.size() >= 5) {
        std::string arg = tokens[4];

        response = "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
      } else if (cmd == "SET" && tokens.size() >= 7){
        std::string var_name = tokens[4];
        std::string var_value = tokens[6];

        CacheEntry entry;
        entry.value = var_value;
        
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
          std::lock_guard<std::mutex> lk(state.mtx_var);
          state.db_var.insert_or_assign(var_name, entry);
        }

        response = "+OK\r\n";
      } else if (cmd == "GET" && tokens.size() >= 5) {
        std::string look_for = tokens[4];

        {
          std::lock_guard<std::mutex> lk(state.mtx_var);
          auto search = state.db_var.find(look_for);

          if (search != state.db_var.end()) {
            if (search->second.has_exp && std::chrono::system_clock::now() > search->second.exp_time) {
              state.db_var.erase(search);
              response = "$-1\r\n";
            } else {
              std::string val = search->second.value;
              response = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
            }
          } else {
            response = "$-1\r\n";
          }
        }
      } else if (cmd == "RPUSH" && tokens.size() >= 7) {
        std::string l_name = tokens[4];
        int l_size;

        {
          std::lock_guard<std::mutex> lk(state.mtx_list);

          auto &list_ref = state.db_list[l_name];

          for (size_t i = 6; i < tokens.size(); i += 2) {
            std::string l_value = tokens[i];

            list_ref.push_back(l_value);
          }

          l_size = list_ref.size();
        }

        response = ":" + std::to_string(l_size) + "\r\n";
      } else if (cmd == "LRANGE" && tokens.size() >= 8) {
        // Positive indexes
        std::string l_name = tokens[4];
        size_t start_idx = std::stoi(tokens[6]);
        size_t end_idx   = std::stoi(tokens[8]);
        std::string empty_arr = "*0\r\n";

        if (start_idx > end_idx) { response = empty_arr; goto exit; }

        {
          std::lock_guard<std::mutex> lk(state.mtx_list);
          auto search = state.db_list.find(l_name);
          if (search == state.db_list.end()) { response = empty_arr; goto exit; }
          auto &list_ref = search->second;
          
          if (start_idx >= list_ref.size()) { response = empty_arr; goto exit; }
          if (end_idx >= list_ref.size())   { end_idx = list_ref.size() - 1; }

          size_t range = end_idx - start_idx + 1;
          response = "*" + std::to_string(range) + "\r\n";

          for (size_t i = start_idx; i <= end_idx; ++i) {
            response += "$" + std::to_string(list_ref[i].length()) + "\r\n";
            response += list_ref[i] + "\r\n";
          }
        }
      }
      else { // default answ
        response = "-ERR unknown command\r\n";
      }
    }

exit:
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
