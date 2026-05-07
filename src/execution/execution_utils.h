#pragma once

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/common.h"
#include "record/rm_file_handle.h"

namespace exec_utils {

inline const ColMeta &find_col(const std::vector<ColMeta> &cols, const TabCol &target) {
    auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) {
        return col.tab_name == target.tab_name && col.name == target.col_name;
    });
    if (pos == cols.end()) {
        throw ColumnNotFoundError(target.tab_name + "." + target.col_name);
    }
    return *pos;
}

inline const char *field_ptr(const std::vector<ColMeta> &cols, const RmRecord *rec, const TabCol &target) {
    const auto &col = find_col(cols, target);
    return rec->data + col.offset;
}

inline std::string normalize_string(const char *data, int len) {
    std::string value(data, len);
    while (!value.empty() && value.back() == '\0') {
        value.pop_back();
    }
    return value;
}

inline int compare_cells(const char *lhs, int lhs_len, const char *rhs, int rhs_len, ColType type) {
    switch (type) {
        case TYPE_INT: {
            int left_value = *reinterpret_cast<const int *>(lhs);
            int right_value = *reinterpret_cast<const int *>(rhs);
            return (left_value < right_value) ? -1 : ((left_value > right_value) ? 1 : 0);
        }
        case TYPE_FLOAT: {
            float left_value = *reinterpret_cast<const float *>(lhs);
            float right_value = *reinterpret_cast<const float *>(rhs);
            return (left_value < right_value) ? -1 : ((left_value > right_value) ? 1 : 0);
        }
        case TYPE_STRING: {
            std::string left_value = normalize_string(lhs, lhs_len);
            std::string right_value = normalize_string(rhs, rhs_len);
            if (left_value < right_value) {
                return -1;
            }
            if (left_value > right_value) {
                return 1;
            }
            return 0;
        }
        default:
            throw InternalError("Unexpected data type");
    }
}

inline bool satisfy_condition(const Condition &cond, const std::vector<ColMeta> &cols, const RmRecord *rec) {
    const auto &lhs_col = find_col(cols, cond.lhs_col);
    const char *lhs_data = rec->data + lhs_col.offset;

    const char *rhs_data = nullptr;
    int rhs_len = lhs_col.len;
    if (cond.is_rhs_val) {
        rhs_data = cond.rhs_val.raw->data;
        rhs_len = cond.rhs_val.raw->size;
    } else {
        const auto &rhs_col = find_col(cols, cond.rhs_col);
        rhs_data = rec->data + rhs_col.offset;
        rhs_len = rhs_col.len;
    }

    int cmp = compare_cells(lhs_data, lhs_col.len, rhs_data, rhs_len, lhs_col.type);
    switch (cond.op) {
        case OP_EQ:
            return cmp == 0;
        case OP_NE:
            return cmp != 0;
        case OP_LT:
            return cmp < 0;
        case OP_GT:
            return cmp > 0;
        case OP_LE:
            return cmp <= 0;
        case OP_GE:
            return cmp >= 0;
        default:
            throw InternalError("Unexpected comparison operator");
    }
}

inline bool satisfy_conds(const std::vector<Condition> &conds, const std::vector<ColMeta> &cols, const RmRecord *rec) {
    for (const auto &cond : conds) {
        if (!satisfy_condition(cond, cols, rec)) {
            return false;
        }
    }
    return true;
}

inline std::unique_ptr<RmRecord> project_record(const RmRecord *src, const std::vector<ColMeta> &src_cols,
                                                const std::vector<ColMeta> &dst_cols,
                                                const std::vector<size_t> &sel_idxs, size_t len) {
    auto result = std::make_unique<RmRecord>(static_cast<int>(len));
    for (size_t i = 0; i < sel_idxs.size(); ++i) {
        const auto &src_col = src_cols[sel_idxs[i]];
        const auto &dst_col = dst_cols[i];
        memcpy(result->data + dst_col.offset, src->data + src_col.offset, src_col.len);
    }
    return result;
}

inline std::unique_ptr<RmRecord> join_record(const RmRecord *left, const RmRecord *right,
                                             const std::vector<ColMeta> &left_cols,
                                             const std::vector<ColMeta> &right_cols, size_t len,
                                             size_t left_len) {
    auto result = std::make_unique<RmRecord>(static_cast<int>(len));
    for (const auto &col : left_cols) {
        memcpy(result->data + col.offset, left->data + col.offset, col.len);
    }
    for (const auto &col : right_cols) {
        memcpy(result->data + left_len + col.offset, right->data + col.offset, col.len);
    }
    return result;
}

}  // namespace exec_utils