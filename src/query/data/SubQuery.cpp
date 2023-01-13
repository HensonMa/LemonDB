
#include "SubQuery.h"

#include "../../multi_thread/multi_thread.h"
#include "../../db/Database.h"
#include "../QueryResult.h"
#include <iostream>

constexpr const char *SubQuery::qname;
static SubQuery *copy_query;
static std::vector<std::string> *copy_operand;
static Table* copy_table;
static int total_thread_num;
static int div_num;

#define MAX_LINE 2000

int subWorker_sub(int idx){
    auto it = copy_table->begin()+idx*div_num;
    auto end_it = (idx == total_thread_num-1) ? copy_table->end() : it + div_num;
    int count = 0;
    while(it != end_it){
        if(copy_query->evalCondition(*it)){
            auto it_field = copy_operand->begin();
            int ans = (*it)[*it_field];
            it_field++;
            for(;it_field < copy_operand->end()-1;it_field++){
                ans -= (*it)[*it_field];
            }
            (*it)[*(it_field++)] = ans;
            count++;
        }
        it ++ ;
    }
    return count;
}

QueryResult::Ptr SubQuery::execute() {
    using namespace std;
    if (this->operands.size() <= 1)
        return make_unique<ErrorMsgResult>(qname, this->targetTable.c_str(),
                                           "Invalid number of operands (? operands), should be larger than 1."_f %
                                           operands.size());
    Database &db = Database::getInstance();
    try {
        auto &table = db[this->targetTable];
        std::pair<std::string, bool> result = initCondition(table);
        int count = 0;
        copy_query  = this;
        copy_operand = &this->operands;
        copy_table = &table;
        total_thread_num = (int) thread_pool.count_idle_thread()+1;
        if(result.second){
            if ((total_thread_num <= 1) || (table.size() < MAX_LINE)){
                div_num = (int) table.size();
                count = subWorker_sub(0);
            } else {
                div_num =  (int) table.size() / total_thread_num;
                vector<future<int>> results((unsigned) total_thread_num-1);
                for (int i = 0; i < total_thread_num-1; i++){
                    results[(unsigned)i] = thread_pool.worker(subWorker_sub, i);
                }
                count += subWorker_sub((int)total_thread_num-1);
                for (int i = 0; i < total_thread_num-1; i++)
                    count = count + results[(unsigned) i].get();
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

std::string SubQuery::toString() {
    return "QUERY = SUB " + this->targetTable + "\"";
}

