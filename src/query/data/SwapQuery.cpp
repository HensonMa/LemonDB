//
// Created by lyj on 22-11-2.
//

#include "SwapQuery.h"
#include "../../db/Database.h"
#include "../../multi_thread/multi_thread.h"
#define MAX_LINE 2000

constexpr const char *SwapQuery::qname;
static int total_thread_num;
static int div_num;
static Table *copy_table;
static ComplexQuery *copy_this;
static std::pair<std::string, bool> result;

int subWorker_swap(int idx, size_t fid1, size_t fid2) {
  auto it = copy_table->begin() + idx * div_num;
  auto end_it =
      (idx == total_thread_num - 1) ? copy_table->end() : it + div_num;
  int count = 0;
  if (result.second) {
    for (auto its = it; its != end_it; ++its) {
      if (copy_this->evalCondition(*its)) {
        if (fid1 != fid2) std::swap((*its)[fid1], (*its)[fid2]);
        count++;
      }
    }
  }
  return count;
}

QueryResult::Ptr SwapQuery::execute() {
    using namespace std;
    Database &db = Database::getInstance();
    try{
      auto &table = db[this->targetTable];
      result = initCondition(table);
      int count = 0;
      total_thread_num = (int) thread_pool.count_idle_thread()+1;
      this->fid1 = table.getFieldIndex(this->operands[0]);
      this->fid2 = table.getFieldIndex(this->operands[1]);
      //auto result = initCondition(table);
        if (total_thread_num <= 1 || table.size() < MAX_LINE) {
            if (result.second) {
                for (auto it = table.begin(); it != table.end(); ++it) {
                    if (this->evalCondition(*it)) {
                        if (fid1 != fid2) swap((*it)[this->fid1], (*it)[this->fid2]);
                        ++count;
                    }
                }
            }
        }
        else {
          copy_table = &table;
          copy_this = this;
          div_num = (int) table.size() / total_thread_num;
          vector<future<int>> futures((unsigned) total_thread_num-1);
          for (int i = 0; i < total_thread_num-1; i++) {
            futures[(unsigned) i] = thread_pool.worker(subWorker_swap, i, fid1, fid2);
          }
          count += subWorker_swap((int)total_thread_num-1,fid1,fid2);
          for (int i = 0; i < total_thread_num-1; i++) {
            count = count + futures[(unsigned)i].get();
          }
        }
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

std::string SwapQuery::toString() {
    return "Query = Swap" + this->targetTable + "\"";
}

