#include "ClearTableQuery.h"
#include "../../db/Database.h"

constexpr const char *ClearTableQuery::qname;

QueryResult::Ptr ClearTableQuery::execute() {
    using namespace std;
    Database &db = Database::getInstance();
    try {
        db.clearTable(this->targetTable);
        return make_unique<SuccessMsgResult>(qname);
    } catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, targetTable, "No such table."s);
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, e.what());
    }
}

std::string ClearTableQuery::toString() {
    return "QUERY = TRUNCATE, Table = \"" + targetTable + "\"";
}
