//
// Created by liu on 18-10-25.
//

#include "UpdateQuery.h"
#include "../../multi_thread/multi_thread.h"
#include "../QueryResult.h"
#include "../../db/Database.h"

constexpr const char *UpdateQuery::qname;
static Table* copy_table;
static UpdateQuery *copy_query;
static int total_thread_num;
static int div_num;
static size_t cnt;
static std::mutex within_m;
static std::string copy_keyvalue;
static int copy_fieldvalue;
static size_t copy_fieldid;


#define MAX_LINE 2000

void subWorker_upt(int idx){
    auto it = copy_table->begin()+idx*div_num;
    auto end_it = (idx == total_thread_num-1) ? copy_table->end() : it + div_num;
    size_t sub_cnt = 0;
    while(it != end_it){
        if(copy_query->evalCondition(*it)){
          if (copy_keyvalue.empty()) {
            (*it)[copy_fieldid] = copy_fieldvalue;
          } else {
            it->setKey(copy_keyvalue);
          }
          ++sub_cnt;
        }
        it ++ ;
    }
    within_m.lock();
    cnt+=sub_cnt;
    within_m.unlock();
}


QueryResult::Ptr UpdateQuery::execute() {
  using namespace std;
  if (this->operands.size() != 2)
    return make_unique<ErrorMsgResult>(
        qname, this->targetTable.c_str(),
        "Invalid number of operands (? operands)."_f % operands.size());
  Database &db = Database::getInstance();
  cnt = 0;
  try {
    auto &table = db[this->targetTable];
    copy_query  = this;
    copy_table = &table;
    total_thread_num = (int) thread_pool.count_idle_thread()+1;
    if (this->operands[0] == "KEY") {
      this->keyValue = this->operands[1];
    } else {
      this->fieldId = table.getFieldIndex(this->operands[0]);
      this->fieldValue =
          (Table::ValueType)strtol(this->operands[1].c_str(), nullptr, 10);
    }
    copy_fieldid = this->fieldId;
    copy_fieldvalue = this->fieldValue;
    copy_keyvalue = this->keyValue;
    auto result = initCondition(table);
    if (result.second) {
      if ((total_thread_num <= 1) || (table.size() < MAX_LINE)){
        div_num = (int) table.size();
        subWorker_upt(0);
      } else {
        div_num =  (int) table.size() / total_thread_num;
        vector<future<void>> results((unsigned) total_thread_num-1);
        for (int i = 0; i < total_thread_num-1; i++){
          results[(unsigned)i] = thread_pool.worker(subWorker_upt, i);
        }
        subWorker_upt((int)total_thread_num-1);
        for (int i = 0; i < total_thread_num-1; i++)
          results[(unsigned) i].get();
      }
    }
    return make_unique<RecordCountResult>(cnt);
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

std::string UpdateQuery::toString() {
  return "QUERY = UPDATE " + this->targetTable + "\"";
}
