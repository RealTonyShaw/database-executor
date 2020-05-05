/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "executor.h"

#include <functional>
#include <string>
#include <iostream>
#include <ctime>
#include <regex>
#include <exceptions/invalid_record_exception.h>
#include <exceptions/invalid_page_exception.h>
#include <unordered_map>

#include "storage.h"
#include "page_iterator.h"
#include "file_iterator.h"

using namespace std;

namespace badgerdb {

    void TableScanner::print() const {
        // TODO: Printer the contents of the table
        std::cout << "Table Name: " << this->tableSchema.getTableName() << std::endl;
        std::cout << "File Name: " << this->tableFile.filename() << std::endl;
        // 打印属性
        for (int i = 0; i < this->tableSchema.getAttrCount(); i++) {
            std::cout << setw(10) << this->tableSchema.getAttrName(i);
        }
        std::cout << std::endl;
        // TODO: 找到如何读取剩余 slot 数并切换 page 的方法

        File file = (File &) this->tableFile;
        Page *page;
        PageId pid = 1;
        for (int m = 1; m < 100; m++) {
            pid = m;
            try {
                bufMgr->readPage(const_cast<File *>(&this->tableFile), pid, page);
            } catch (InvalidPageException &) {
                break;
            }
            for (int i = 1; i < 1000; i++) {
                // TODO: 怎么判断已经没有 record 了？
                try {
                    std::cout << page->getRecord({static_cast<PageId>(m), static_cast<SlotId>(i)}).c_str();
                    string tmp = page->getRecord({static_cast<PageId>(m), static_cast<SlotId>(i)}).c_str();
                    vector<string> tmpVec = JoinOperator::tupleParser(tmp, const_cast<TableSchema &>(this->tableSchema));
                    cout << "Parser Result: ";
                    for (string a: tmpVec) {
                        cout << a << " ";
                    }
                    cout << endl;
                    std::cout << std::endl;
                } catch (InvalidRecordException &) {
                    break;
                }
            }
            // Unpin 读取过的页面
            // bufMgr->unPinPage(&file, pid, false);
            bufMgr->unPinPage(const_cast<File *>(&this->tableFile), pid, false);
        }

    }

    // 删除 leftTableFile 和 rightTableFile 的 const 修饰
    JoinOperator::JoinOperator(File &leftTableFile,
                               File &rightTableFile,
                               const TableSchema &leftTableSchema,
                               const TableSchema &rightTableSchema,
                               const Catalog *catalog,
                               BufMgr *bufMgr)
            : leftTableFile(leftTableFile),
              rightTableFile(rightTableFile),
              leftTableSchema(leftTableSchema),
              rightTableSchema(rightTableSchema),
              resultTableSchema(
                      createResultTableSchema(leftTableSchema, rightTableSchema)),
              catalog(catalog),
              bufMgr(bufMgr),
              isComplete(false) {
        // nothing
    }

    TableSchema JoinOperator::createResultTableSchema(
            const TableSchema &leftTableSchema,
            const TableSchema &rightTableSchema) {
        cout << "Left Schema: ";
        for (int i = 0; i < leftTableSchema.getAttrCount(); i++) {
            cout << leftTableSchema.getAttrName(i) << " ";
        }
        cout << endl;
        cout << "Right Schema: ";
        for (int i = 0; i < rightTableSchema.getAttrCount(); i++) {
            cout << rightTableSchema.getAttrName(i) << " ";
        }
        cout << endl;
        vector<Attribute> attrs;
        // TODO: add attribute definitions
        for (int i = 0; i < leftTableSchema.getAttrCount(); i++) {
            Attribute attribute(leftTableSchema.getAttrName(i), leftTableSchema.getAttrType(i), leftTableSchema.getAttrMaxSize(i), leftTableSchema.isAttrNotNull(i), leftTableSchema.isAttrUnique(i));
            attrs.emplace_back(attribute);
        }
        for (int i = 0; i < rightTableSchema.getAttrCount(); i++) {
            bool overlapped = false;
            for (int j = 0; j < leftTableSchema.getAttrCount(); j++) {
                if (leftTableSchema.getAttrName(j) == rightTableSchema.getAttrName(i)) {
                    // 有重叠时
                    overlapped = true;
                }
            }
            if (overlapped) {
                continue;
            }
            Attribute attribute(rightTableSchema.getAttrName(i), rightTableSchema.getAttrType(i), rightTableSchema.getAttrMaxSize(i), rightTableSchema.isAttrNotNull(i), rightTableSchema.isAttrUnique(i));
            attrs.emplace_back(attribute);
        }
        cout << "New Schema: ";
        for (Attribute a: attrs) {
            cout << a.attrName << " ";
        }
        cout << endl;
        return TableSchema("TEMP_TABLE", attrs, true);
    }

    void JoinOperator::printRunningStats() const {
        cout << "# Result Tuples: " << numResultTuples << endl;
        cout << "# Used Buffer Pages: " << numUsedBufPages << endl;
        cout << "# I/Os: " << numIOs << endl;
    }

    /**
    OnePassJoinOperator::hashSearch(const string &key) const {

    }
     **/

    bool OnePassJoinOperator::execute(int numAvailableBufPages, File &resultFile) {
        if (isComplete)
            return true;

        numResultTuples = 0;
        numUsedBufPages = 0;
        numIOs = 0;

        // TODO: Execute the join algorithm
        cout << "Execute the join algorithm" << endl;
        cout << "File Name: " << leftTableFile.filename() << endl;
        // 用于存储查找结构的数据结构，hashMap 序列
        vector<std::unordered_map<int, std::string>> hashMap;
        // 用于保存左表的 page
        Page *page;
        PageId pid = 1;
        bufMgr->clearBufStats();
        // TODO: 加入对 left 和 right 表哪个更小的判断

        // 外关系
        File leftFile = this->leftTableFile;
        for (FileIterator iter = leftFile.begin();
             iter != leftFile.end();
             ++iter) {
            // 页查找结构
            std::unordered_map<int, std::string> tmpMap;
            // Iterate through all records on the page.
            for (PageIterator page_iter = (*iter).begin();
                 page_iter != (*iter).end();
                 ++page_iter) {
                // std::cout << "Found record: " << *page_iter
                //          << " on page " << (*iter).page_number() << "\n";
                string tmp = *page_iter;
                vector<string> tmpVec = JoinOperator::tupleParser(tmp, const_cast<TableSchema &>(this->leftTableSchema));
                attrContentParserForChar(tmpVec[1]);
                int num = attrContentParserForInt(tmpVec[2]);
                cout << "Left Table Content: " << "Char: " << tmpVec[1] << " " << "Int: " << num << endl;
                tmpMap.insert(make_pair(num, tmpVec[1]));
            }
            cout << "Size: " << tmpMap.size() << endl;
            hashMap.emplace_back(tmpMap);
        }
        cout << "File Name: " << rightTableFile.filename() << endl;

        // 内关系
        File rightFile = this->rightTableFile;
        for (FileIterator iter = rightFile.begin();
             iter != rightFile.end();
             ++iter) {
            // Iterate through all records on the page.
            for (PageIterator page_iter = (*iter).begin();
                 page_iter != (*iter).end();
                 ++page_iter) {
                //std::cout << "Found record: " << *page_iter
                //          << " on page " << (*iter).page_number() << "\n";
                string tmp = *page_iter;
                vector<string> tmpVec = JoinOperator::tupleParser(tmp, const_cast<TableSchema &>(this->rightTableSchema));
                int num = attrContentParserForInt(tmpVec[1]);
                attrContentParserForVarchar(tmpVec[2]);

                for (std::unordered_map<int, std::string> map: hashMap) {
                    std::unordered_map<int, std::string>::iterator it;
                    if ((it = map.find(num)) != map.end()) {
                        cout << "To Be Joined: " << num << " " << tmpVec[2] << " " << it->second << endl;
                    }
                }

                cout << "Right Table Content: " << "Int: " << num << " " << "VerChar: " << tmpVec[2] << endl;
            }
        }

        isComplete = true;
        return true;
    }

    bool NestedLoopJoinOperator::execute(int numAvailableBufPages, File &resultFile) {
        if (isComplete)
            return true;

        numResultTuples = 0;
        numUsedBufPages = 0;
        numIOs = 0;

        // TODO: Execute the join algorithm

        isComplete = true;
        return true;
    }

    BucketId GraceHashJoinOperator::hash(const string &key) const {
        std::hash<string> strHash;
        return strHash(key) % numBuckets;
    }

    bool GraceHashJoinOperator::execute(int numAvailableBufPages, File &resultFile) {
        if (isComplete)
            return true;

        numResultTuples = 0;
        numUsedBufPages = 0;
        numIOs = 0;

        // TODO: Execute the join algorithm

        isComplete = true;
        return true;
    }

    void JoinOperator::attrContentParserForChar(string &attrContent) {
        attrContent.substr(0, attrContent.find_first_of('?'));
    }

    void JoinOperator::attrContentParserForVarchar(string &attrContent) {
        attrContent.substr(0, attrContent.find_first_of('^'));
    }

    int JoinOperator::attrContentParserForInt(string &attrContent) {
        return stoi(attrContent.substr(0, attrContent.find_first_of('?')));
    }

    vector<string> JoinOperator::tupleParser(string &tuple, TableSchema &tableSchema) {
        vector<string> result;
        string header = tuple.substr(0, 8);
        result.emplace_back(header);
        int ptr = 8;
        for (int i = 0; i < tableSchema.getAttrCount(); i++) {
            if (tableSchema.getAttrType(i) == VARCHAR) {
                for (int j = ptr; j < tuple.length(); ++j) {
                    if (tuple[j] == '^') {
                        result.emplace_back(tuple.substr(ptr, j - ptr + 1));
                        ptr = j + 1;
                        break;
                    }
                }
            } else {
                result.emplace_back(tuple.substr(ptr, tableSchema.getAttrMaxSize(i)));
                ptr += tableSchema.getAttrMaxSize(i);
            }
        }
        return result;
    }


}  // namespace badgerdb