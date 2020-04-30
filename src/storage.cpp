/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "storage.h"
#include <regex>
#include <sstream>
#include <exceptions/insufficient_space_exception.h>

using namespace std;

namespace badgerdb {

    PageId pidOfLastTime;
    Page* pageOfLastTime;
    bool assigned = false;
    int totalPageNumber = 0;
    File* fileOfLastTime;
    bool fileAssigned = false;

    RecordId HeapFileManager::insertTuple(const string &tuple,
                                          File &file,
                                          BufMgr *bufMgr) {
        // TODO
        // PageId pid;
        // Page* page;
        // File* fileOfRDB = &file;
        if (!fileAssigned) {
            fileOfLastTime = &file;
            fileAssigned = true;
        }
        // TODO: 检查需要如何填充是更换了 file 文件（一个想法：使用 filename）
        if (!assigned) {
            // bufMgr->allocPage(fileOfRDB, pid, page);
            bufMgr->allocPage(&file, pidOfLastTime, pageOfLastTime);
            assigned = true;
            totalPageNumber++;
        }
        RecordId rid{};
        try {
            rid = pageOfLastTime->insertRecord(tuple);
        } catch (InsufficientSpaceException &) {
            bufMgr->allocPage(&file, pidOfLastTime, pageOfLastTime);
            totalPageNumber++;
            rid = pageOfLastTime->insertRecord(tuple);
        }
        cout << "RID: " << (int &)rid.page_number << " " << (int &)rid.slot_number << " " << tuple << " File: " << file.filename() << endl;
        // bufMgr->unPinPage(fileOfRDB, pidOfLastTime, true);
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
        char tuple[20];
        // 如果是条 Insert 语句
        // TODO: 抛出一个不符合 Insert 语句的异常
        if (regex_match(sql, result, insertStatementPattern)) {
            // 保存表名
            tableName = result[1];
            // 获取 Table 的 schema
            TableId tableId = catalog->getTableId(tableName);
            const TableSchema& schema = catalog->getTableSchema(tableId);
            // 保存属性声明信息
            attrInfo = result[2];
            vector<string> outcome;
            // 按 , 分割 tuple 插入信息，并保存至 outcome
            HeapFileManager::divideByChar(result[2], ',', outcome);
            // tuple 的格式为 (tablename attr1value, attr2value, .....)
            sprintf((char *)(&tuple), "%s %s,", tableName.c_str(), attrInfo.c_str());
            // 分割内部表示
            smatch result2;
            vector<string> midResult;
            for (int i = 0; i < outcome.size(); i++) {
                if (regex_match(outcome[i], result2, charPattern)) {
                    std::cout << result2[1] << std::endl;
                    midResult.emplace_back(result2[1]);
                }
                if (regex_match(outcome[i], result2, intPattern)) {
                    std::cout << result2[1] << std::endl;
                    midResult.emplace_back(result2[1]);
                }
            }
            /**
            for (auto &s : outcome) {
                if (regex_match(s, result2, charPattern)) {
                    std::cout << result2[1] << std::endl;
                    midResult.emplace_back(result2[1]);
                }
                if (regex_match(s, result2, intPattern)) {
                    std::cout << result2[1] << std::endl;
                    midResult.emplace_back(result2[1]);
                }
            }
             **/
        }
        cout << "TUPLE: " << tuple << endl;
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

    int HeapFileManager::roundUpSize(const string& str) {
        int length = str.size();
        if (length % 4 != 0) {
            return (length / 4 + 1) * 4;
        }
        return length / 4 * 4;
    }

    int HeapFileManager::getSizeOfTuple(const Catalog *catalog, const string& name) {
        TableId tableId = catalog->getTableId(name);
        const TableSchema& schema = catalog->getTableSchema(tableId);

    }
}  // namespace badgerdb