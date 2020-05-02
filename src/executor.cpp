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

#include "storage.h"
#include "page_iterator.h"
#include "file_iterator.h"

using namespace std;

namespace badgerdb {

    void TableScanner::print() const {
        // TODO: Printer the contents of the table
        std::cout << "Table Name: " << this->tableSchema.getTableName() << std::endl;
        std::cout << "File Name: " << this->tableFile.filename() << std::endl;
        /**
        regex generalPattern(R"([ ]*(.*))", regex::icase);
        regex tuplePatter(R"((.*)[ ](.*))", regex::icase);
         **/
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
            cout << "NotHere";
            for (int i = 1; i < 1000; i++) {
                // TODO: 怎么判断已经没有 record 了？
                try {
                    std::cout << page->getRecord({1, static_cast<SlotId>(i)}).c_str();
                    std::cout << std::endl;
                } catch (InvalidRecordException &) {
                    break;
                }
            }
            bufMgr->unPinPage(&file, pid, false);
        }
    }

    JoinOperator::JoinOperator(const File &leftTableFile,
                               const File &rightTableFile,
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
        vector<Attribute> attrs;
        // TODO: add attribute definitions
        return TableSchema("TEMP_TABLE", attrs, true);
    }

    void JoinOperator::printRunningStats() const {
        cout << "# Result Tuples: " << numResultTuples << endl;
        cout << "# Used Buffer Pages: " << numUsedBufPages << endl;
        cout << "# I/Os: " << numIOs << endl;
    }

    bool OnePassJoinOperator::execute(int numAvailableBufPages, File &resultFile) {
        if (isComplete)
            return true;

        numResultTuples = 0;
        numUsedBufPages = 0;
        numIOs = 0;

        // TODO: Execute the join algorithm

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

}  // namespace badgerdb