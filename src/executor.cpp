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

#include "storage.h"
#include "page_iterator.h"
#include "file_iterator.h"

using namespace std;

namespace badgerdb {

    void TableScanner::print() const {
        // TODO: Printer the contents of the table
        std::cout << "Table Name: " << this->tableSchema.getTableName() << std::endl;
        // std::cout << "Count: " << this->tableSchema.getAttrCount() << std::endl;
        std::cout << "File Name: " << this->tableFile.filename() << std::endl;
        regex generalPattern(R"([ ]*(.*))", regex::icase);
        regex tuplePatter(R"((.*)[ ](.*))", regex::icase);
        for (int i = 0; i < this->tableSchema.getAttrCount(); i++) {
            std::cout << setw(10) << this->tableSchema.getAttrName(i);
        }
        std::cout << std::endl;
        File file = (File &) this->tableFile;
        for (FileIterator iter = file.begin(); iter != file.end(); ++iter) {
            for (PageIterator page_iter = (*iter).begin(); page_iter != (*iter).end(); ++page_iter) {
//                std::cout << "Found record: " << *page_iter
//                          << " on page " << (*iter).page_number() << "\n";
                vector<string> attributes;
                HeapFileManager::divideByChar(*page_iter, ',', attributes);
                smatch result;
                for (auto &s: attributes) {
                    std::cout << s << std::endl;
                    if (regex_match(s, result, tuplePatter)) {
                        // std::cout << setw(10) << result[2];
                        continue;
                    }
                    if (regex_match(s, result, generalPattern)) {
                        // std::cout << setw(10) << result[1];
                    }
                }
            }
            std::cout << std::endl;

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