#pragma once

#include "duckdb.hpp"

namespace duckdb {

string IntervalToISOString(const interval_t &interval);
bool IntervalFromISOString(const char *str, idx_t len, interval_t &result);

} // namespace duckdb
