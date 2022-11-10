#include "duckdb.hpp"

using namespace duckdb;

int main()
{
	DBConfig default_config;
	default_config.options.allow_unsigned_extensions = true;

	DuckDB db(nullptr, &default_config);

	// DuckDB db(nullptr);

	Connection con(db);

	// auto rv_ext = con.Query("select * From duckdb_extensions();");
	// rv_ext->Print();

	auto tb_rv = con.Query("CREATE TABLE variants(i INTEGER, v VARIANT)");
	tb_rv->Print();
	con.Query("INSERT INTO variants VALUES (3, variant(123))");
	con.Query("INSERT INTO variants VALUES (4, variant(DATE '1992-09-20'))");
	auto result = con.Query("SELECT * FROM variants");
	result->Print();

}
