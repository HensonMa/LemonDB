#include "CountQuery.h"

#include <algorithm>

#include "../../db/Database.h"
#include "../../multi_thread/multi_thread.h"
#include "../QueryResult.h"

constexpr const char *CountQuery::qname;
static CountQuery *copy_query;
static int cnt;
static Table* copy_table;
static int total_thread_num;
static int div_num;
static std::mutex within_m;

#define MAX_LINE 2000

void subWorker_cnt(int idx){
  auto it = copy_table->begin()+idx*div_num;
  auto end_it = (idx == total_thread_num-1) ? copy_table->end() : it + div_num;
  int sub_cnt = 0;
  while(it!=end_it){
    if(copy_query->evalCondition(*it)){
      sub_cnt++;
    }
    it++;
  }
  within_m.lock();
  cnt+=sub_cnt;
  within_m.unlock();
}
QueryResult::Ptr CountQuery::execute(){
    using namespace std;
//    if (this->operands.size() < 0)
//        return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
//                                       "No operand (? operands)."_f %
//                                           operands.size());
    Database &db = Database::getInstance();
    try {
      cnt = 0;
      auto &table = db[this->targetTable];
      std::pair<std::string, bool> result = initCondition(table);
      copy_query  = this;
      copy_table = &table;
      total_thread_num = (int) thread_pool.count_idle_thread()+1;
      if(result.second){
        if ((total_thread_num <= 1) || (table.size() < MAX_LINE)){
          div_num = (int) table.size();
          subWorker_cnt(0);
        } else {
          div_num =  (int) table.size() / total_thread_num;
          vector<future<void>> results((unsigned) total_thread_num-1);
          for (int i = 0; i < total_thread_num-1; i++){
            results[(unsigned)i] = thread_pool.worker(subWorker_cnt, i);
          }
          subWorker_cnt((int)total_thread_num-1);
          for (int i = 0; i < total_thread_num-1; i++)
            results[(unsigned) i].get();
          }
        }
        return make_unique<RecordAnsResult>(cnt);
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

std::string CountQuery::toString() {
    return "QUERY = COUNT " + this->targetTable + "\"";
}
