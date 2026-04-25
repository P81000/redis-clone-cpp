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
#include <condition_variable>
#include <variant>

struct StreamEntry {
  std::string id;
  std::unordered_map<std::string, std::string> fields;
};

struct CacheEntry {
  std::string value;
  std::chrono::time_point<std::chrono::system_clock> exp_time;
  bool has_exp = false;
};

using RedisStream = std::vector<StreamEntry>;
using RedisList = std::vector<std::string>;
using RedisValue = std::variant<CacheEntry, RedisList, RedisStream>;

struct ServerState {
  std::mutex db_mtx;
  std::unordered_map<std::string, RedisValue> db;

  std::condition_variable list_block;
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
          std::lock_guard<std::mutex> lk(state.db_mtx);
          state.db.insert_or_assign(var_name, entry);
        }

        response = "+OK\r\n";
      } else if (cmd == "GET" && tokens.size() >= 5) {
        std::string look_for = tokens[4];

        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(look_for);

          if (search == state.db.end()) { response = "$-1\r\n"; goto exit; }
          if (not std::holds_alternative<CacheEntry>(search->second)) {
            response = "-WRONGTYPE Operation against a key holding the wrong kind of value";
            goto exit;
          }

          auto& entry = std::get<CacheEntry>(search->second);
          if (entry.has_exp && std::chrono::system_clock::now() > entry.exp_time) {
            state.db.erase(search);
            response = "$-1\r\n";
          } else {
            std::string val = entry.value;
            response = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
          }
        }
      } else if (cmd == "RPUSH" && tokens.size() >= 7) {
        std::string l_name = tokens[4];
        int l_size = 0;

        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(l_name);

          if (search == state.db.end()) {
            state.db[l_name] = RedisList();
            auto& list_ref = std::get<RedisList>(state.db[l_name]);

            for (size_t i = 6; i < tokens.size(); i += 2) {
              list_ref.push_back(tokens[i]);
            }

            l_size = list_ref.size();
            state.list_block.notify_all();
          }

          if (std::holds_alternative<RedisList>(search->second)) {
            auto& list_ref = std::get<RedisList>(search->second);

            for (size_t i = 6; i < tokens.size(); i += 2) {
              list_ref.push_back(tokens[i]);
            }

            l_size = list_ref.size();
            state.list_block.notify_all();

          }

          if (search != state.db.end() && not std::holds_alternative<RedisList>(search->second)) {
            response = "-WRONGTYPE Operation against a key holding the wrong kind of value";
          }
        }

        response = ":" + std::to_string(l_size) + "\r\n";
      } else if (cmd == "LPUSH" && tokens.size() >= 7) {
        std::string l_name = tokens[4];
        int l_size;
        
        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(l_name);

          if (search != state.db.end() && not std::holds_alternative<RedisList>(search->second)) {
            response = "-WRONGTYPE Operation against a key holding the wrong kind of value";
            goto exit;
          }

          if (search == state.db.end()) {
            state.db[l_name] = RedisList();
            auto& list_ref = std::get<RedisList>(state.db[l_name]);

            for (size_t i = 6; i < tokens.size(); i += 2) {
              list_ref.insert(list_ref.begin(), tokens[i]);
            }

            l_size = list_ref.size();
            state.list_block.notify_all();
          }

          if (std::holds_alternative<RedisList>(search->second)) {
            auto& list_ref = std::get<RedisList>(state.db[l_name]);

            for (size_t i = 6; i < tokens.size(); i += 2) {
              list_ref.insert(list_ref.begin(), tokens[i]);
            }

            l_size = list_ref.size();
            state.list_block.notify_all();
          }
        }

        response = ":" + std::to_string(l_size) + "\r\n";
      } else if (cmd == "LRANGE" && tokens.size() >= 9) {
        // Positive indexes
        std::string l_name = tokens[4];
        int start_idx = std::stoi(tokens[6]);
        int end_idx   = std::stoi(tokens[8]);
        std::string empty_arr = "*0\r\n";

        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(l_name);

          if (search != state.db.end() && not std::holds_alternative<RedisList>(search->second)) {
            response = "-WRONGTYPE Operation against a key holding the wrong kind of value";
            goto exit;
          }

          if (search == state.db.end()) { response = empty_arr; goto exit; }

          auto &list_ref = std::get<RedisList>(search->second);
          int size = list_ref.size();
          
          if (start_idx < 0) start_idx = size + start_idx;
          if (end_idx < 0) end_idx = size + end_idx;

          if (start_idx < 0) start_idx = 0;
          if (end_idx >= size) end_idx = size - 1;

          if (start_idx > end_idx || start_idx >= size) { response = empty_arr; goto exit; }

          int range = end_idx - start_idx + 1;
          response = "*" + std::to_string(range) + "\r\n";

          for (int i = start_idx; i <= end_idx; ++i) {
            response += "$" + std::to_string(list_ref[i].length()) + "\r\n";
            response += list_ref[i] + "\r\n";
          }
        }
      } else if (cmd == "LLEN" && tokens.size() >= 5) {
        std::string l_name = tokens[4];
        
        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(l_name);

          if (search != state.db.end() && not std::holds_alternative<RedisList>(search->second)) {
            response = "-WRONGTYPE Operation against a key holding the wrong kind of value";
            goto exit;
          }

          if (search == state.db.end()) { response = ":0\r\n"; goto exit; }

          auto &list_ref = std::get<RedisList>(search->second);
          int size = list_ref.size();

          response = ":" + std::to_string(size) + "\r\n";
        }
      } else if (cmd == "LPOP" && tokens.size() >= 5) {
        std::string l_name = tokens[4];
        int items_to_pop = 1;
        bool return_arr = false;

        if (tokens.size() >= 7) {
          return_arr = true;
          if (std::stoi(tokens[6]) < 0) { response = "-ERR value is out of range, must be positive"; goto exit; }
          items_to_pop = std::stoi(tokens[6]);
        }

        {
          std::lock_guard<std::mutex> lk(state.db_mtx);

          auto search = state.db.find(l_name);
          if (search != state.db.end() && not std::holds_alternative<RedisList>(search->second)) {
            response = "-WRONGTYPE Operation against a key holding the wrong kind of value";
            goto exit;
          }

          auto& list_ref = std::get<RedisList>(search->second);

          if (search == state.db.end() || list_ref.size() == 0) { response = "$-1\r\n"; goto exit; }
          if (items_to_pop >= (int)list_ref.size()) { items_to_pop = list_ref.size(); }

          response = "";

          if (return_arr) {
            response = "*" + std::to_string(items_to_pop) + "\r\n";
          }

          for (int i = 0; i < items_to_pop; ++i) {
            response += "$" + std::to_string(list_ref.front().length()) + "\r\n";
            response += list_ref.front() + "\r\n";

            list_ref.erase(list_ref.begin());
          }

          if (list_ref.empty()) {
            state.db.erase(l_name);
          }
        }
      } else if (cmd == "BLPOP" && tokens.size() >= 7) {
        std::string l_name = tokens[4];
        double timeout = std::stod(tokens[6]);

        {
          std::unique_lock<std::mutex> lk(state.db_mtx);

          auto search = state.db.find(l_name);
          auto& list_ref = std::get<RedisList>(search->second);

          if (search != state.db.end() && not std::holds_alternative<RedisList>(search->second)) {
            response = "-WRONGTYPE Operation against a key holding the wrong kind of value";
            goto exit;
          }

          auto has_data = [&]() {
              return search != state.db.end() && !list_ref.empty();
          };

          bool success = false;

          if (timeout == 0.0) {
            state.list_block.wait(lk, has_data);
            success = true;
          } else {
            auto max_wait = std::chrono::duration<double>(timeout);
            success = state.list_block.wait_for(lk, max_wait, has_data);
          }

          if (!success) {
            response = "*-1\r\n";
          } else {
            std::string popped_value = list_ref.front();
            list_ref.erase(list_ref.begin());

            if (list_ref.empty()) {
              state.db.erase(l_name);
            }

            response = "*2\r\n";
            response += "$" + std::to_string(l_name.length()) + "\r\n" + l_name + "\r\n";
            response += "$" + std::to_string(popped_value.length()) + "\r\n" + popped_value + "\r\n";
          }
        }
      } else if (cmd == "TYPE" && tokens.size() >= 5) {
        std::string obj_name = tokens[4];

        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(obj_name);
          if (search == state.db.end()) { response = "+none\r\n"; goto exit; }

          if (std::holds_alternative<RedisList>(search->second)) { response = "+list\r\n"; goto exit; }
          if (std::holds_alternative<RedisStream>(search->second)) { response = "+stream\r\n"; goto exit; }
          if (std::holds_alternative<CacheEntry>(search->second)) { response = "+string\r\n"; goto exit; }
        }
      } else { // default answ
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
