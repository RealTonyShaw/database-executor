/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "schema.h"
#include <regex>
#include <string>
#include <sstream>
#include <iostream>
#include <iomanip>

using namespace std;

namespace badgerdb {

    TableSchema TableSchema::fromSQLStatement(const string &sql) {
        string tableName;
        // 保存属性初始化
        string attributeInitStatements;
        vector<string> attributeInit;
        const char div = ',';
        const char div2 = ' ';
        vector<Attribute> attrs;
        bool isTemp = false;
        // TODO: create attribute definitions from sql
        // 能兼容大小写混用的情况
        regex SQLStatementPattern("CREATE TABLE (.*) \\((.*)\\);", regex::icase);
        regex charPattern(R"(CHAR\((\d+)\))", regex::icase);
        regex varcharPattern(R"(VARCHAR\((\d+)\))", regex::icase);
        smatch result;
        if (regex_match(sql, result, SQLStatementPattern)) {
            tableName = result[1];
            attributeInitStatements = result[2];
        }
        // std::cout << "TableName: " << tableName << " Attributes: " << attributeInitStatements << std::endl;
        istringstream stringStream(attributeInitStatements);
        string tmpString;
        // attributeInit 保存每段属性初始化命令
        while (getline(stringStream, tmpString, div)) {
            attributeInit.emplace_back(move(tmpString));
        }
        // 分割每次 attributeInit 中的命令
        for (const auto& s : attributeInit) {
            // std::cout << s << std::endl;
            istringstream stringStreamTmp(s);
            string tmpString2;
            vector<string> value;
            while (getline(stringStreamTmp, tmpString2, div2)) {
                string next = move(tmpString2);
                if (!next.empty()) {
                    value.emplace_back(next);
                }
            }
            string attrName = value[0];
            bool isNotNull = false;
            bool isUnique = false;
            int maxSize;
            DataType attrType;
            for (const auto& s2 : value) {
                // std::cout << s2 << std::endl;
                if (s2 == "NOT") {
                    isNotNull = true;
                }
                if (s2 == "UNIQUE") {
                    isUnique = true;
                }
                if (s2 == "INT") {
                    attrType = INT;
                    maxSize = 4;
                }
                smatch resultTmp;
                if (regex_match(s2, resultTmp, varcharPattern)) {
                    attrType = VARCHAR;
                    maxSize = stoi(resultTmp[1]);
                    resultTmp.empty();
                }
                if (regex_match(s2, resultTmp, charPattern)) {
                    attrType = CHAR;
                    maxSize = stoi(resultTmp[1]);
                    resultTmp.empty();
                }
            }
            Attribute attribute(attrName, attrType, maxSize, isNotNull, isUnique);
            attrs.emplace_back(attribute);
        }
        return TableSchema(tableName, attrs, isTemp);
    }

    void TableSchema::print() const {
        // TODO
        std::cout << "Table Name: " << this->tableName << std::endl;
        std::cout << setw(10) << "Name" << setw(10) << "Type" << setw(10) << "Length" << setw(10) << std::endl;
        for (auto& a : this->attrs) {
            std::cout << setw(10) << a.attrName << setw(10) << TypeToString(a.attrType) << setw(10) << a.maxSize << std::endl;
        }
    }

    std::string TableSchema::TypeToString(DataType dt) {
        switch (dt) {
            case INT:
                return "INT";
            case CHAR:
                return "CHAR";
            case VARCHAR:
                return "VARCHAR";
            default:
                return "UNDEFINED";
        }
    }

    std::string TableSchema::BoolToString(bool b) {
        return b ? "True" : "False";
    }

}  // namespace badgerdb