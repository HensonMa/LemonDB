//
// Created by lyj on 22-11-2.
//

#ifndef LEMONDB_SWAPQUERY_H
#define LEMONDB_SWAPQUERY_H

#include "../Query.h"

class SwapQuery : public ComplexQuery {
    static constexpr const char *qname = "SWAP";
    Table::FieldIndex fid1, fid2; // Field index fid1, fid2 to be swapped

public:
    using ComplexQuery::ComplexQuery;

    QueryResult::Ptr execute() override;

    std::string toString() override;
};

#endif //LEMONDB_SWAPQUERY_H
