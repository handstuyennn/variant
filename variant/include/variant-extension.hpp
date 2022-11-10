#pragma once

#include "duckdb.hpp"

namespace duckdb {

class VariantExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	string Name() override;

private:
	void LoadVariant(Connection &con);
};

} // namespace duckdb
