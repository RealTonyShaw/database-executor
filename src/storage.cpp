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
        if (fileOfLastTime != &file) {
            fileOfLastTime = &file;
            assigned = false;
        }
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
        Page* page;
        bugMgr->readPage(&file, rid.page_number, page);
        page->deleteRecord(rid);
        bugMgr->unPinPage(&file, page->page_number(), true);
    }

    string HeapFileManager::createTupleFromSQLStatement(const string &sql,
                                                        const Catalog *catalog) {
        // TODO
        // 正则匹配
        regex insertStatementPattern(R"(INSERT INTO (.+) VALUES \((.+)\);)");
        regex charPattern(R"([ ]*'(.*)')", regex::icase);
        regex intPattern(R"([ ]*(\d+))", regex::icase);
        smatch result;
        // Insert 语句段落划分
        string tableName;
        string attrInfo;
        vector<unsigned char> tuple;
        // TODO: header 加入 bitmap
        vector<unsigned char> header = {'0', '0', '0', '0', '0', '0', '0', '0'};
        concatCharVector(tuple, header);
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
            // 分割内部表示
            smatch result2;
            // 创建游标
            int flag = 0;
            for (int i = 0; i < outcome.size(); i++) {
                if (regex_match(outcome[i], result2, charPattern)) {
                    std::cout << result2[1] << std::endl;
                    if (schema.getAttrType(i) == VARCHAR) { // 在 VARCHAR 类型的情况下，大小补全至 4 的倍数
                        vector<unsigned char> varcharTmp;
                        // result2[1] = result[1] + '^';
                        string tmpResult = result2[1];
                        tmpResult += '^';
                        // insertStringIntoCharArray(varcharTmp, result2[1], roundUpSize(result2[1]));
                        insertStringIntoCharArray(varcharTmp, tmpResult, roundUpSize(tmpResult));
                        concatCharVector(tuple, varcharTmp);
                    } else if (schema.getAttrType(i) == CHAR) {
                        vector<unsigned char> charTmp;
                        insertStringIntoCharArray(charTmp, result2[1], schema.getAttrMaxSize(i));
                        concatCharVector(tuple, charTmp);
                    } else {
                        //TODO: 类型识别错误异常
                    }
                }
                if (regex_match(outcome[i], result2, intPattern)) {
                    std::cout << result2[1] << std::endl;
                    if (schema.getAttrType(i) == INT) {
                        // int intAttr = stoi(result2[i]);
                        vector<unsigned char> intTmp;
                        // vector<unsigned char> attrOfByte = intToBytes(intAttr);
                        insertStringIntoCharArray(intTmp, result2[1], schema.getAttrMaxSize(i));
                        // insertCharArrayIntoCharArray(intTmp, attrOfByte, schema.getAttrMaxSize(i));
                        concatCharVector(tuple, intTmp);
                        // concatCharVector(tuple, attrOfByte);
                        cout << "tuple: ";
                        for (unsigned char t: tuple) {
                            cout << t;
                        }
                        cout << endl;
                    } else {
                        //TODO: 类型识别错误异常
                    }
                }
            }
        }
        // 最后一位留作 '\0'
        char tupleOfCharType[tuple.size() + 1];
        convertVectorToArray(tuple, tupleOfCharType);
        // cout << "Tuple Content: " << string(tupleOfCharType) << endl;
        return tupleOfCharType;
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

    vector<unsigned char> HeapFileManager::intToBytes(int integer) {
        vector<unsigned char> bytes(4);
        for (int i = 0; i < 4; i++)
            bytes[3 - i] = (integer >> (i * 8));
        return bytes;
    }

    int HeapFileManager::bytesToInt(vector<unsigned char> bytes) {
        int originalInt = bytes[0] << 24 | (int)bytes[1] << 16 | (int)bytes[2] << 8 | (int)bytes[3];
        return originalInt;
    }

    void HeapFileManager::insertStringIntoCharArray(vector<unsigned char>& originalChar, const string& originalString, int size) {
        for (int i = 0; i < size; i++) {
            // 用 '?' 代替数据库中的 NULL
            originalChar.emplace_back('?');
        }
        for (int i = 0; i < originalString.length(); i++) {
            originalChar[i] = originalString[i];
        }
    }

    void HeapFileManager::insertCharArrayIntoCharArray(vector<unsigned char>& originalChar, vector<unsigned char>& toBeInserted, int size) {
        for (int i = 0; i < size; i++) {
            // 用 '?' 代替数据库中的 NULL
            originalChar.emplace_back('?');
        }
        for (int i = 0; i < toBeInserted.size(); i++) {
            originalChar[i] = toBeInserted[i];
        }
    }

    void HeapFileManager::concatCharVector(vector<unsigned char>& a, vector<unsigned char>& b) {
        a.insert(a.end(), b.begin(), b.end());
    }

    void HeapFileManager::convertVectorToArray(vector<unsigned char>& v, char array[]) {
        for (int i = 0; i < v.size(); i++) {
            array[i] = v[i];
        }
        array[v.size()] = '\0';
    }
}  // namespace badgerdb