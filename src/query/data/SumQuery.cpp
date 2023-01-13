#include "SumQuery.h"
#include "../../multi_thread/multi_thread.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

constexpr const char *SumQuery::qname;
static SumQuery *copy_query;
static std::vector<int> rst_arr;
static std::vector<std::string> *copy_operand;
static Table* copy_table;
static int total_thread_num;
static int div_num;
static std::mutex within_m;

#define MAX_LINE 2000

void subWorker_sum(int idx){
  auto it = copy_table->begin()+idx*div_num;
  auto end_it = (idx == total_thread_num-1) ? copy_table->end() : it + div_num;
  std::vector<int> sub_rst_arr(copy_operand->size(),0);
  while(it!=end_it){
    if(copy_query->evalCondition(*it)){
      for(unsigned int i=0; i<copy_operand->size();++i){
        sub_rst_arr[i] += (*it)[(*copy_operand)[i]];
      }
    }
    it++;
  }
  within_m.lock();
  for(unsigned int i=0; i<copy_operand->size();++i){
    rst_arr[i]+=sub_rst_arr[i];
  }
  within_m.unlock();
}

QueryResult::Ptr SumQuery::execute(){
    using namespace std;
    if (this->operands.size() <= 0)
        return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                           "Invalid number of operands (? operands), should be larger than 1."_f %
                                           operands.size());
    Database &db = Database::getInstance();
    try {
      auto &table = db[this->targetTable];
      std::pair<std::string, bool> result = initCondition(table);
      rst_arr.clear();
      rst_arr.resize((this->operands).size(),0);
      copy_query  = this;
      copy_operand = &this->operands;
      copy_table = &table;
      total_thread_num = (int) thread_pool.count_idle_thread()+1;
      if(result.second){
        if ((total_thread_num <= 1) || (table.size() < MAX_LINE)){
          div_num = (int) table.size();
          subWorker_sum(0);
        } else {
          div_num =  (int) table.size() / total_thread_num;
          vector<future<void>> results((unsigned) total_thread_num-1);
          for (int i = 0; i < total_thread_num-1; i++){
            results[(unsigned)i] = thread_pool.worker(subWorker_sum, i);
          }
          subWorker_sum((int)total_thread_num-1);
          for (int i = 0; i < total_thread_num-1; i++)
            results[(unsigned) i].get();
          }
        }
        return make_unique<RecordAnsResult>(rst_arr);
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

std::string SumQuery::toString() {
    return "Query = Sum" + this->targetTable + "\"";
}
