#pragma once
#include <iostream>
#include <map>
#include <stddef.h>
#include <string>
#include <vector>

class CSVParserBase {
public:
    virtual int BeginOfRow(size_t row_id) = 0;
    virtual int EndOfRow(size_t row_id) = 0;
    virtual int LoadValue(size_t row_id, int field_id, const char* buf, int size) = 0;
};

class EmptyCSVParser : public CSVParserBase {
public:
    virtual int BeginOfRow(size_t row_id) override {
        return 0;
    }
    virtual int EndOfRow(size_t row_id) override {
        return 0;
    }
    virtual int LoadValue(size_t row_id, int field_id, const char* buf, int size) override {
        return 0;
    }
};

class EchoCSVParser : public CSVParserBase {
public:
    uint64_t sum = 0;
    virtual int BeginOfRow(size_t row_id) override {
        // std::cout << "begin: " << row_id << std::endl;
        return 0;
    }
    virtual int EndOfRow(size_t row_id) override {
        // std::cout << "end: " << row_id << std::endl;
        return 0;
    }
    virtual int LoadValue(size_t row_id, int field_id, const char* buf, int size) override {
        // std::cout << "\t" << row_id << " " << field_id << " " << std::string(buf, size) << std::endl;
        if (field_id == 2) {
            sum += atoi(buf);
        }

        return 0;
    }
};