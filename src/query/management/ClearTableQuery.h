
#ifndef PROJECT_CLEARTABLEQUERY_H
#define PROJECT_CLEARTABLEQUERY_H

#include "../Query.h"

class ClearTableQuery : public Query {
    static constexpr const char *qname = "TRUNCATE";

public:
    explicit ClearTableQuery(std::string table) : Query(std::move(table)) {}
    QueryResult::Ptr execute() override;
    std::string toString() override;
};


#endif //PROJECT_CLEARTABLEQUERY_H
