#ifndef PROJECT_COPYTABLEQUERY_H
#define PROJECT_COPYTABLEQUERY_H

#include "../Query.h"

class CopyTableQuery : public Query {
  static constexpr const char *qname = "COPYTABLE";
  const std::string targetedTable;//the table is going to be copied into

public:
  explicit CopyTableQuery(std::string table, std::string fileName)
      : Query(std::move(table)), targetedTable(std::move(fileName)) {}

  QueryResult::Ptr execute() override;

  std::string toString() override;
};

#endif // PROJECT_COPYTABLEQUERY_H
