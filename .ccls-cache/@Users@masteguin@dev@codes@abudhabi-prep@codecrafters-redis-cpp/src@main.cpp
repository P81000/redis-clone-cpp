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
#include <deque>

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
using RedisList = std::deque<std::string>;
using RedisValue = std::variant<CacheEntry, RedisList, RedisStream>;

struct ServerState {
  std::mutex db_mtx;
  std::unordered_map<std::string, RedisValue> db;

  std::condition_variable list_block;
};

std::vector<std::string_view> parse_resp(const std::string& input) {
  std::vector<std::string_view> tokens;
  size_t start = 0;
  size_t end = input.find("\r\n");

  while (end != std::string::npos) {
    tokens.emplace_back(input.data() + start, end - start);
    start = end + 2;
    end = input.find("\r\n", start);
  }

  return tokens;
}

void handle_client(int client_fd, ServerState& state) {
  char buffer[1024];
  std::string response = "";
  response.reserve(512);
  static const std::string empty_arr = "*0\r\n";

  for (;;) {
    response.clear();

    int bytes = recv(client_fd, buffer, sizeof(buffer), 0);
    if (bytes <= 0) {
      std::cout << "Client disconnected.\n";
      break;
    }
    
    std::string request(buffer, bytes);
    std::vector<std::string_view> tokens = parse_resp(request);

//     for (auto t : tokens) std::cout << "[ debug ] " << t << std::endl;

    if (tokens.size() >= 3) {
      std::string cmd(tokens[2]);

      for (auto& c : cmd) c = toupper(c);

      if (cmd == "PING") {
        response.append("+PONG\r\n");
      } else if (cmd == "ECHO" && tokens.size() >= 5) {
        std::string_view arg = tokens[4];

        response.append("$");
        response.append(std::to_string(arg.length()));
        response.append("\r\n");
        response.append(arg);
        response.append("\r\n");
      } else if (cmd == "SET" && tokens.size() >= 7){
        std::string var_name(tokens[4]);

        CacheEntry entry;
        entry.value = tokens[6];
        
        if (tokens.size() >= 11) {
          std::string op(tokens[8]);
          for (auto& c : op) c = toupper(c);

          int duration = std::stoi(std::string(tokens[10]));

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

        response.append("+OK\r\n");
      } else if (cmd == "GET" && tokens.size() >= 5) {
        std::string look_for(tokens[4]);

        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(look_for);

          if (search == state.db.end()) { response.append("$-1\r\n"); goto exit; }
          if (not std::holds_alternative<CacheEntry>(search->second)) {
            response.append("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
            goto exit;
          }

          auto& entry = std::get<CacheEntry>(search->second);
          if (entry.has_exp && std::chrono::system_clock::now() > entry.exp_time) {
            state.db.erase(search);
            response.append("$-1\r\n");
          } else {
            auto& val = entry.value;

            response.append("$");
            response.append(std::to_string(val.length()));
            response.append("\r\n");
            response.append(val);
            response.append("\r\n");
          }
        }
      } else if (cmd == "RPUSH" && tokens.size() >= 7) {
        std::string l_name(tokens[4]);
        int l_size = 0;

        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(l_name);

          if (search != state.db.end() && not std::holds_alternative<RedisList>(search->second)) {
            response.append("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
          }

          auto [it, inserted] = state.db.try_emplace(l_name, RedisList{});
          auto& list_ref = std::get<RedisList>(it->second);

          for (size_t i = 6; i < tokens.size(); i += 2) {
            list_ref.emplace_back(tokens[i]);
          }

          l_size = list_ref.size();
          state.list_block.notify_all();
        }

        response = ":" + std::to_string(l_size) + "\r\n";
      } else if (cmd == "LPUSH" && tokens.size() >= 7) {
        std::string l_name(tokens[4]);
        int l_size;
        
        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(l_name);

          if (search != state.db.end() && not std::holds_alternative<RedisList>(search->second)) {
            response.append("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
            goto exit;
          }

          if (search == state.db.end()) {
            search = state.db.emplace(l_name, RedisList{}).first;
          }

          auto& list_ref = std::get<RedisList>(state.db[l_name]);

          for (size_t i = 6; i < tokens.size(); i += 2) {
            list_ref.emplace_front(tokens[i]);
          }

          l_size = list_ref.size();
          state.list_block.notify_all();
        }

        response = ":" + std::to_string(l_size) + "\r\n";
      } else if (cmd == "LRANGE" && tokens.size() >= 9) {
        // Positive indexes
        std::string l_name(tokens[4]);
        int start_idx = std::stoi(std::string(tokens[6]));
        int end_idx   = std::stoi(std::string(tokens[8]));

        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(l_name);

          if (search != state.db.end() && not std::holds_alternative<RedisList>(search->second)) {
            response.append("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
            goto exit;
          }

          if (search == state.db.end()) { response.append(empty_arr); goto exit; }

          auto &list_ref = std::get<RedisList>(search->second);
          int size = list_ref.size();
          
          if (start_idx < 0) start_idx = size + start_idx;
          if (end_idx < 0) end_idx = size + end_idx;

          if (start_idx < 0) start_idx = 0;
          if (end_idx >= size) end_idx = size - 1;

          if (start_idx > end_idx || start_idx >= size) { response = empty_arr; goto exit; }

          int range = end_idx - start_idx + 1;
          response.append("*");
          response.append(std::to_string(range));
          response.append("\r\n");

          for (int i = start_idx; i <= end_idx; ++i) {
            response.append("$");
            response.append(std::to_string(list_ref[i].length()));
            response.append("\r\n");
            response.append(list_ref[i]);
            response.append("\r\n");
          }
        }
      } else if (cmd == "LLEN" && tokens.size() >= 5) {
        std::string l_name(tokens[4]);
        
        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(l_name);

          if (search != state.db.end() && not std::holds_alternative<RedisList>(search->second)) {
            response.append("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
            goto exit;
          }

          if (search == state.db.end()) { response = ":0\r\n"; goto exit; }

          auto &list_ref = std::get<RedisList>(search->second);
          int size = list_ref.size();

          response.append(":");
          response.append(std::to_string(size));
          response.append("\r\n");
        }
      } else if (cmd == "LPOP" && tokens.size() >= 5) {
        std::string l_name(tokens[4]);
        int items_to_pop = 1;
        bool return_arr = false;

        if (tokens.size() >= 7) {
          return_arr = true;
          if (std::stoi(std::string(tokens[6])) < 0) { response.append("-ERR value is out of range, must be positive"); goto exit; }
          items_to_pop = std::stoi(std::string(tokens[6]));
        }

        {
          std::lock_guard<std::mutex> lk(state.db_mtx);

          auto search = state.db.find(l_name);
          if (search != state.db.end() && not std::holds_alternative<RedisList>(search->second)) {
            response.append("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
            goto exit;
          }
          if (search == state.db.end()) { response.append("$-1\r\n"); goto exit; }

          auto& list_ref = std::get<RedisList>(search->second);

          if (list_ref.size() == 0) { response.append("$-1\r\n"); goto exit; }
          if (items_to_pop >= (int)list_ref.size()) { items_to_pop = list_ref.size(); }

          if (return_arr) {
            response.append("*");
            response.append(std::to_string(items_to_pop));
            response.append("\r\n");
          }

          for (int i = 0; i < items_to_pop; ++i) {
            response.append("$");
            response.append(std::to_string(list_ref.front().length()));
            response.append("\r\n");
            response.append(list_ref.front());
            response.append("\r\n");

            list_ref.pop_front();
          }

          if (list_ref.empty()) {
            state.db.erase(l_name);
          }
        }
      } else if (cmd == "BLPOP" && tokens.size() >= 7) {
        std::string l_name(tokens[4]);
        double timeout = std::stod(std::string(tokens[6]));

        {
          std::unique_lock<std::mutex> lk(state.db_mtx);
          decltype(state.db.end()) search;

          search = state.db.find(l_name);

          if (search != state.db.end() && not std::holds_alternative<RedisList>(search->second)) {
            response.append("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
            goto exit;
          }

          auto has_data = [&]() {
            search = state.db.find(l_name);
            if (search == state.db.end()) return false;
            if (not std::holds_alternative<RedisList>(search->second)) return false;

            auto& list_ref = std::get<RedisList>(search->second);
            return !list_ref.empty();
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
            response.append("*-1\r\n");
          } else {
            auto& list_ref = std::get<RedisList>(search->second);
            auto& popped_value = list_ref.front();
            list_ref.pop_front();

            if (list_ref.empty()) {
              state.db.erase(search);
            }

            response.append("*2\r\n");
            response.append("$");
            response.append(std::to_string(l_name.length()));
            response.append("\r\n");
            response.append(l_name);
            response.append("\r\n");
            response.append("$");
            response.append(std::to_string(popped_value.length()));
            response.append("\r\n");
            response.append(popped_value);
            response.append("\r\n");
          }
        }

        // 1 2 XADD 4 stream_key 5 1526919030474-0 7 temperature 9 36 11 humidity 13 95
        // 0 1  2   3     4      5        6        7      8      9 10 11    12    13 14
      } else if (cmd == "XADD" && tokens.size() >= 11){
        std::string stream_name(tokens[4]);

        StreamEntry stream_entry;
        stream_entry.id = tokens[6];

        for (size_t i = 8; i < tokens.size(); i += 4) {
          if (i + 2 < tokens.size()) {
            stream_entry.fields[std::string(tokens[i])] = tokens[i + 2];
          }
        }

        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(stream_name);

          if (search != state.db.end() && not std::holds_alternative<RedisStream>(search->second)) {
            response.append("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
            goto exit;
          }

          if (search == state.db.end()) {
            state.db[stream_name] = RedisStream();
          }

          auto& stream_ref = std::get<RedisStream>(state.db[stream_name]);
          stream_ref.emplace_back(stream_entry);
       }

        response.append("$");
        response.append(std::to_string(stream_entry.id.length()));
        response.append("\r\n");
        response.append(stream_entry.id);
        response.append("\r\n");
      } else if (cmd == "TYPE" && tokens.size() >= 5) {
        std::string obj_name(tokens[4]);

        {
          std::lock_guard<std::mutex> lk(state.db_mtx);
          auto search = state.db.find(obj_name);
          if (search == state.db.end()) { response.append("+none\r\n"); goto exit; }

          if (std::holds_alternative<RedisList>(search->second)) { response.append("+list\r\n"); goto exit; }
          if (std::holds_alternative<RedisStream>(search->second)) { response.append("+stream\r\n"); goto exit; }
          if (std::holds_alternative<CacheEntry>(search->second)) { response.append("+string\r\n"); goto exit; }
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
