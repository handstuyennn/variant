#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"

#include "variant-extension.hpp"

namespace duckdb {

void VariantExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	LoadVariant(con);
	con.Commit();
}

string VariantExtension::Name() {
	return "variant";
}

} // namespace duckdb
