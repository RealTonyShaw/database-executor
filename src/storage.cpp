/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "storage.h"
#include <regex>
#include <sstream>

using namespace std;

namespace badgerdb {

    RecordId HeapFileManager::insertTuple(const string &tuple,
                                          File &file,
                                          BufMgr *bufMgr) {
        // TODO
        PageId pid;
        Page* page;
        File* fileOfRDB = &file;
        bufMgr->allocPage(fileOfRDB, pid, page);
        RecordId rid{};
        rid = page->insertRecord(tuple);
        bufMgr->unPinPage(fileOfRDB, pid, true);
    }

    void HeapFileManager::deleteTuple(const RecordId &rid,
                                      File &file,
                                      BufMgr *bugMgr) {
        // TODO
    }

    string HeapFileManager::createTupleFromSQLStatement(const string &sql,
                                                        const Catalog *catalog) {
        // TODO
        regex insertStatementPattern(R"(INSERT INTO (.+) VALUES \((.+)\);)");
        regex charPattern(R"([ ]*'(.*)')", regex::icase);
        regex intPattern(R"([ ]*(\d+))", regex::icase);
        smatch result;
        string tableName;
        string attrInfo;
        char tuple[100];
        if (regex_match(sql, result, insertStatementPattern)) {
            tableName = result[1];
            attrInfo = result[2];
            vector<string> outcome;
            HeapFileManager::divideByChar(result[2], ',', outcome);
            std::cout << "Table: " << result[1] << " Content: " << result[2] << std::endl;
            // tuple 的格式为 (tablename attr1value, attr2value, .....)
            sprintf((char *)(&tuple), "%s %s,", tableName.c_str(), attrInfo.c_str());

            /**
            smatch result2;
            for (auto &s : outcome) {
                if (regex_match(s, result2, charPattern)) {
                    std::cout << result2[1] << std::endl;
                }
                if (regex_match(s, result2, intPattern)) {
                    std::cout << result2[1] << std::endl;
                }
            }
             */
        }
        return tuple;
    }

    void HeapFileManager::divideByChar(const string& str, char div, vector<string> &outcome) {
        outcome.clear();
        istringstream stringStreamTmp(str);
        string tmpString;
        while (getline(stringStreamTmp, tmpString, div)) {
            string next = move(tmpString);
            if (!next.empty()) {
                outcome.emplace_back(next);
            }
        }
    }

}  // namespace badgerdb