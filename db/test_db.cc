#include "db_manager.h"
#include "msgpack_helper.h"
#include <iostream>
#include <fstream>
#include <cstdlib>

using namespace tdchunk;


std::string FileName(const std::string& dbname, uint64_t number,
                                const char* suffix) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "/%06llu.%s",
                static_cast<unsigned long long>(number), suffix);
  return dbname + buf;
}

int main(int argc, char *argv[]) {
  float eperc = std::atof(argv[1]);

  std::vector<std::string> db_paths;
  for (int i = 0; i < 26; i++) {
    std::string path = "/home/nsccgz_qylin_1/checkpoint/incrcp/emb_l." + std::to_string(i) + ".weight";
    db_paths.push_back(path);
  }

  DBManager* db_manager = new DBManager();
  db_manager->OpenDBs(db_paths, false, eperc);

  db_manager->GetCheckpointFiles(0, 1);

  db_manager->ReleaseDBs();
  delete db_manager;

  return 0;
}

