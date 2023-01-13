//
// Created by Ying on 22-11-4.
//

// for DELETE

#include "DeleteQuery.h"

#include <algorithm>

#include "../../db/Database.h"
#include "../../multi_thread/multi_thread.h"

constexpr const char *DeleteQuery::qname;
static int total_thread_num;
static int div_num;
static Table* copy_table;
static ComplexQuery* copy_query;
static std::pair<std::string, bool> result;
#define MAX_LINE 2000

std::vector<Table::KeyType> keys;
std::mutex mutex_keys;
static Table::SizeType counter;

void subWorker_delete(int idx){
  auto it = copy_table->begin() + idx*div_num;
  auto end_it = (idx == total_thread_num-1) ? copy_table->end() : it + div_num;
  if(result.second) {
    for(auto its = it; its != end_it; its++){
      if(copy_query->evalCondition(*its)){
        auto key = its->key();
        mutex_keys.lock();
        keys.push_back(key);
        counter++;
        mutex_keys.unlock();
      }
    }
  }
}

QueryResult::Ptr DeleteQuery::execute() {
    using namespace std;
    if (!this->operands.empty())
        return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(), "Invalid number of operands (? operands)."_f % operands.size());
    Database &db = Database::getInstance();
    try {
        auto &table = db[this->targetTable];
        total_thread_num = (int) thread_pool.count_idle_thread()+1;
        result = initCondition(table);
        counter = 0;
        if (result.second) {
            if (this->condition.empty()) {
                counter = table.size();
                table.clear();
            } else if ((total_thread_num <= 1) || (table.size() < MAX_LINE)) {
              for (auto it = table.begin(); it != table.end(); ++it) {
                if (this->evalCondition(*it)) {
                  table.deleteByIndex(it->key());
                  counter++;
                  it--;
                }
              }
            } else {
              keys.clear();
              copy_query  = this;
              copy_table = &table;
              div_num = (int) table.size() / total_thread_num;
              vector<std::future<void>> futures((unsigned) total_thread_num);
              for (int i = 0; i < total_thread_num; i++) {
                futures[(unsigned) i] = thread_pool.worker(subWorker_delete, i);
              }
              for (int i = 0; i < total_thread_num; i++) {
                futures[(unsigned) i].get();
              }
              for (auto & key : keys) {
                table.deleteByIndex(key);
              }
            }
        }
        return make_unique<RecordCountResult>(counter);
    } catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'."_f % e.what());
    }
}

std::string DeleteQuery::toString() {
    return "QUERY = DELETE " + this->targetTable + "\"";
}
