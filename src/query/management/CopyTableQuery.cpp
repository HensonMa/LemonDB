#include "CopyTableQuery.h"

#include "../../db/Database.h"

constexpr const char *CopyTableQuery::qname;

QueryResult::Ptr CopyTableQuery::execute() {
  using namespace std;
  Database &db = Database::getInstance();
  try {
    auto &table = db.get_copied_table(targetedTable);
    table.copy(db[targetTable]);
    return make_unique<SuccessMsgResult>(qname, targetTable);
  } catch (const exception &e) {
    return make_unique<ErrorMsgResult>(qname, e.what());
  }
}

std::string CopyTableQuery::toString() {
  return "QUERY = Copy TABLE, TABLE = \"" + this->targetTable + "\", " + "To Table = \"" + this->targetedTable + "\"";
}
