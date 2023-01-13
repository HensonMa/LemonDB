#include "../../multi_thread/multi_thread.h"
#include "../QueryResult.h"
#include <algorithm>
#include "../../db/Database.h"
#include "MaxQuery.h"

constexpr const char *MaxQuery::qname;
static MaxQuery *copy_query;
static std::vector<int> rst_arr;
static std::vector<std::string> *copy_operand;
static Table* copy_table;
static int total_thread_num;
static int div_num;
static std::mutex within_m;
static bool found;

#define MAX_LINE 2000

void subWorker_max(int idx){
  auto it = copy_table->begin()+idx*div_num;
  auto end_it = (idx == total_thread_num-1) ? copy_table->end() : it + div_num;
  std::vector<int> sub_rst_arr(copy_operand->size(),INT32_MIN);
  bool sub_found = false;
  while(it!=end_it){
    if(copy_query->evalCondition(*it)){
      sub_found = true;
      for(unsigned int i=0; i< copy_operand->size();++i){
        if (sub_rst_arr[i] < (*it)[(*copy_operand)[i]]){
          sub_rst_arr[i] = (*it)[(*copy_operand)[i]];
        }
      }
    }
    it++;
  }
  within_m.lock();
  found = (sub_found || found);
  for(unsigned int i=0; i< copy_operand->size();++i){
    if(rst_arr[i]<sub_rst_arr[i]){
      rst_arr[i]=sub_rst_arr[i];
    }
  }
  within_m.unlock();
}

QueryResult::Ptr MaxQuery::execute(){
    using namespace std;
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                       "No operand (? operands)."_f %
                                           operands.size());
    Database &db = Database::getInstance();
    try {
      found = false;
      auto &table = db[this->targetTable];
      std::pair<std::string, bool> result = initCondition(table);
      rst_arr.clear();
      rst_arr.resize((this->operands).size(),INT32_MIN);
      copy_query  = this;
      copy_operand = &this->operands;
      copy_table = &table;
      total_thread_num = (int) thread_pool.count_idle_thread()+1;
      if(result.second){
        if ((total_thread_num <= 1) || (table.size() < MAX_LINE)){
          div_num = (int) table.size();
          subWorker_max(0);
        } else {
          div_num =  (int) table.size() / total_thread_num;
          vector<future<void>> results((unsigned) total_thread_num-1);
          for (int i = 0; i < total_thread_num-1; i++){
            results[(unsigned)i] = thread_pool.worker(subWorker_max, i);
          }
          subWorker_max((int)total_thread_num-1);
          for (int i = 0; i < total_thread_num-1; i++)
            results[(unsigned) i].get();
          }
        }
        if(found) return make_unique<RecordAnsResult>(rst_arr);
        else return make_unique<NullQueryResult>();
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

std::string MaxQuery::toString() {
  return "QUERY = MAX " + this->targetTable + "\"";
}
