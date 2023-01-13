#include "SelectQuery.h"
#include <queue>
#include "../../db/Database.h"
#include "../../multi_thread/multi_thread.h"
struct key_comparer {
  bool operator()(Table::Iterator &a, Table::Iterator &b) {
    return (a->key() > b->key());
  }
};

static std::pair<std::string, bool> result;
static std::mutex record_mutex;
static unsigned long div_num;
static unsigned long thread_count;
static Table* copy_table;
static ComplexQuery* copy_query;
static std::priority_queue<Table::Iterator, std::vector<Table::Iterator>,
                           key_comparer> records;


void select_thread(unsigned long idx) {
  auto it = copy_table->begin() + static_cast<int>(idx * div_num);
  auto end_it = (idx == thread_count-1) ? copy_table->end() : it + static_cast<int>(div_num);
  if(result.second) {
    for(auto its = it; its != end_it; its++){
      if(copy_query->evalCondition(*its)){
        record_mutex.lock();
        records.push(its);
        record_mutex.unlock();
      }
    }
  }
}

QueryResult::Ptr SelectQuery::execute() {
  using namespace std;
  Database &db = Database::getInstance();
  // start of try
  try {
    auto &table = db[this->targetTable];
    result = initCondition(table);
    thread_count = (unsigned long)thread_pool.count_idle_thread();
    if (thread_count < 2 || table.size() < 2000) {
      if (result.second) {
        for (auto it(table.begin()); it != table.end(); ++it) {
          if (this->evalCondition(*it))
            records.push(it);
        }
      }
    } else {
      thread_count = (unsigned long)(table.size() / 2000 + 1);
      copy_query = this;
      copy_table = &table;
      div_num = (unsigned long)(table.size()) / thread_count;
      vector<std::future<void>> futures((unsigned long)thread_count);
      for (unsigned long i = 0; i < thread_count; i++) {
        futures[i] = thread_pool.worker(select_thread, i);
      }
      for (unsigned long i = 0; i < thread_count; i++) {
        futures[i].get();
      }
    }
    if (records.empty()) {
      return make_unique<NullQueryResult>();
    }
    ostringstream query_result;
    while (!records.empty()) {
      auto top = records.top();
      records.pop();
      query_result << "( ";
      for (auto & operand : this->operands) {
        if (operand == "KEY") {
          query_result << top->key();
        } else {
          query_result << (*top)[operand];
        }
        query_result << " ";
      }
      query_result << ")\n";
    }
    std::string string_result = query_result.str();
    string_result.erase(string_result.size() - 1);
    return make_unique<RecordAnsResult>(string_result);
  } catch (const TableNameNotFound &e) {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  } catch (const IllFormedQueryCondition &e) {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  } catch (const invalid_argument &e) {
    // Cannot convert operand to string
    return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  } catch (const exception &e) {
    return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
  }
}

std::string SelectQuery::toString() {
  return "QUERY = SELECT " + this->targetTable + "\"";
}

