
// Created by luyij on 2022/11/3.


#include "DuplicateQuery.h"
#include "../../db/Database.h"
#include "../../multi_thread/multi_thread.h"

#define MAX_LINE 2000

constexpr const char *DuplicateQuery::qname;
static int total_thread_num;
static int div_num;
static Table* copy_table;
static ComplexQuery* copy_query;
static std::pair<std::string, bool> result;

struct Ret_Dup {
  Table::SizeType subcounter;
  std::vector<Table::KeyType> sub_keys;
};

Ret_Dup subWorker_duplicate(int idx){
  auto it = copy_table->begin() + idx*div_num;
  auto end_it = (idx == total_thread_num-1) ? copy_table->end() : it + div_num;
  Ret_Dup temp;
  temp.subcounter = 0;
  if(result.second) {
    std::vector<Table::KeyType> sub_keys;
    Table::SizeType sub_counter = 0;
    for(auto its = it; its != end_it; its++){
      if(copy_query->evalCondition(*its)){
        auto key = its->key();
        sub_keys.push_back(key);
        sub_counter++;
      }
    }
    temp.sub_keys = sub_keys;
    temp.subcounter = sub_counter;
  }
  return temp;
}

QueryResult::Ptr DuplicateQuery::execute() {
    using namespace std;
    Database &db = Database::getInstance();
    Table::SizeType count = 0;
    try {
        auto &table = db[this->targetTable];
        result = initCondition(table);
        total_thread_num = (int) thread_pool.count_idle_thread()+1;
        vector<Table::KeyType> keys;
        if (result.second) {
            if ((total_thread_num <= 1) || (table.size() < MAX_LINE)) {
                for (auto it = table.begin(); it != table.end(); ++it) {
                    if (this->evalCondition(*it)) {
                        keys.push_back(it->key());
                        ++count;
                    }
                }
            } else {
                copy_query  = this;
                copy_table = &table;
                div_num = (int) table.size() / total_thread_num;
                vector<future<Ret_Dup>> futures((unsigned) total_thread_num);
                for (int i = 0; i < total_thread_num; i++) {
                    futures[(unsigned) i] = thread_pool.worker(subWorker_duplicate, i);
                }
                for (int i = 0; i < total_thread_num; i++) {
                    Ret_Dup temp = futures[(unsigned) i].get();
                    size_t temp_size = keys.size();
                    keys.resize(temp_size + temp.sub_keys.size());
                    std::move(temp.sub_keys.begin(), temp.sub_keys.end(), keys.begin() + (long) temp_size);
                    count = count + temp.subcounter;
                }
            }
        }
        if (count > 0) table.duplicate(keys, count);
        return make_unique<RecordCountResult>(count);
    } catch (const TableNameNotFound &e) {
      return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                         "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
      return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
      // Cannot convert operand to string
      return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                         "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
      return make_unique<ErrorMsgResult>(qname, this->targetTable,
                                         "Unkonwn error '?'."_f % e.what());
    }
}

std::string DuplicateQuery::toString() {
    return "Query = DUPLICATE " + this->targetTable + "\"";
}