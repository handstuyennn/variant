#pragma once

#include <optional>

#include "duckdb.hpp"

namespace duckdb {

class ValueReader {
public:
	ValueReader(const Value &value) noexcept : value(value) {
	}

	bool IsNull() const;
	bool GetBool() const;
	std::optional<int64_t> GetInt() const;
	std::optional<uint64_t> GetUInt() const;
	double GetReal() const;
	const string &GetString() const;
	string GetDecimal() const;
	string GetUUID() const;
	string GetEnum() const;
	string GetDate() const;
	string GetTime() const;
	string GetTimestamp() const;
	string GetDatetime() const;
	const interval_t &GetInterval() const;
	ValueReader operator[](idx_t index) const;
	size_t ListSize() const;

	class ListIterator {
	public:
		using iterator = ListIterator;
		using iterator_category = std::input_iterator_tag;
		using difference_type = std::ptrdiff_t;
		using value_type = ValueReader;
		using pointer = value_type *;
		using reference = value_type;
		using wrapped_iter = vector<Value>::const_iterator;
		// clang-format off
		explicit ListIterator(wrapped_iter it) : it(it) {}
		iterator &operator++() { ++it; return *this; }
		iterator operator++(int) { iterator retval = *this; ++(*this); return retval; }
		bool operator==(iterator other) const { return it == other.it; }
		bool operator!=(iterator other) const { return !(*this == other); }
		reference operator*() const { return ValueReader(*it); }
		// clang-format on
	private:
		wrapped_iter it;
	};
	ListIterator begin() const;
	ListIterator end() const;

private:
	const Value &value;
};

class ValueWriter {
public:
	ValueWriter(Value &value) noexcept : value(value) {
	}

	void SetNull();
	void SetBool(bool v);
	void SetInt32(int32_t v);
	void SetInt64(int64_t v);
	void SetDouble(double v);
	void SetNumeric(const hugeint_t &v);
	void SetInterval(const interval_t &v);
	void SetString(string &&str);
	void SetList();
	ValueWriter ListAppend();
	void SetStruct();
	ValueWriter operator[](idx_t index);

private:
	void SetNotNull();

private:
	Value &value;
};

typedef std::function<bool(ValueWriter &, const ValueReader &)> value_unary_func;
void VectorExecuteUnary(DataChunk &args, Vector &result, value_unary_func func);

typedef std::function<bool(ValueWriter &, const ValueReader &, const ValueReader &)> value_binary_func;
void VectorExecuteBinary(DataChunk &args, Vector &result, value_binary_func func);

} // namespace duckdb
