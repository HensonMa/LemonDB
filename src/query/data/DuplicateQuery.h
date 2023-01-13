//
// Created by luyij on 2022/11/3.
//

#ifndef P2TEAM_03_DUPLICATEQUERY_H
#define P2TEAM_03_DUPLICATEQUERY_H

#include "../Query.h"

class DuplicateQuery : public ComplexQuery {
    static constexpr const char *qname = "DUPLICATE";

public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;
};

#endif //P2TEAM_03_DUPLICATEQUERY_H
