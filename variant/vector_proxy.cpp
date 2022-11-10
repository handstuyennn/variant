#include <optional>

#include "duckdb.hpp"

#include "vector_proxy.hpp"

namespace duckdb {

bool ValueReader::IsNull() const {
	return value.IsNull();
}

bool ValueReader::GetBool() const {
	D_ASSERT(value.type().id() == LogicalTypeId::BOOLEAN && !value.IsNull());
	return value.GetValueUnsafe<bool>();
}

std::optional<int64_t> ValueReader::GetInt() const {
	D_ASSERT(value.type().IsIntegral() && !value.IsNull());
	string error;
	Value new_value;
	if (value.DefaultTryCastAs(LogicalType::BIGINT, new_value, &error)) {
		return new_value.GetValueUnsafe<int64_t>();
	} else {
		return {};
	};
}

std::optional<uint64_t> ValueReader::GetUInt() const {
	D_ASSERT(value.type().IsIntegral() && !value.IsNull());
	string error;
	Value new_value;
	if (value.DefaultTryCastAs(LogicalType::UBIGINT, new_value, &error)) {
		return new_value.GetValueUnsafe<uint64_t>();
	} else {
		return {};
	};
}

double ValueReader::GetReal() const {
	D_ASSERT(!value.IsNull());
	if (value.type().id() == LogicalTypeId::DOUBLE) {
		return value.GetValueUnsafe<double>();
	}
	D_ASSERT(value.type().id() == LogicalTypeId::FLOAT);
	return (double)value.GetValueUnsafe<float>();
}

const string &ValueReader::GetString() const {
	D_ASSERT(value.type().InternalType() == PhysicalType::VARCHAR && !value.IsNull());
	return StringValue::Get(value);
}

string ValueReader::GetDecimal() const {
	D_ASSERT(value.type().id() == LogicalTypeId::DECIMAL && !value.IsNull());
	return StringValue::Get(value.DefaultCastAs(LogicalTypeId::VARCHAR));
}

string ValueReader::GetUUID() const {
	D_ASSERT(value.type().id() == LogicalTypeId::UUID && !value.IsNull());
	return StringValue::Get(value.DefaultCastAs(LogicalTypeId::VARCHAR));
}

string ValueReader::GetEnum() const {
	D_ASSERT(value.type().id() == LogicalTypeId::ENUM && !value.IsNull());
	return StringValue::Get(value.DefaultCastAs(LogicalTypeId::VARCHAR));
}

string ValueReader::GetDate() const {
	D_ASSERT(value.type().id() == LogicalTypeId::DATE && !value.IsNull());
	return StringValue::Get(value.DefaultCastAs(LogicalTypeId::VARCHAR));
}

string ValueReader::GetTime() const {
	return StringValue::Get(value.DefaultCastAs(LogicalTypeId::VARCHAR));
}

string ValueReader::GetTimestamp() const {
	return StringValue::Get(value.DefaultCastAs(LogicalTypeId::VARCHAR));
}

string ValueReader::GetDatetime() const {
	return StringValue::Get(value.DefaultCastAs(LogicalTypeId::VARCHAR));
}

const interval_t &ValueReader::GetInterval() const {
	return const_cast<Value &>(value).GetReferenceUnsafe<interval_t>();
}

ValueReader ValueReader::operator[](idx_t index) const {
	D_ASSERT(value.type().InternalType() == PhysicalType::STRUCT && index < StructValue::GetChildren(value).size());
	return ValueReader(StructValue::GetChildren(value)[index]);
}

size_t ValueReader::ListSize() const {
	D_ASSERT(value.type().id() == LogicalTypeId::LIST);
	return ListValue::GetChildren(value).size();
}

ValueReader::ListIterator ValueReader::begin() const {
	return ListIterator(ListValue::GetChildren(value).cbegin());
}

ValueReader::ListIterator ValueReader::end() const {
	return ListIterator(ListValue::GetChildren(value).cend());
}

void ValueWriter::SetNull() {
	D_ASSERT(value.IsNull());
}

void ValueWriter::SetNotNull() {
	D_ASSERT(value.IsNull());
	LogicalType type = value.type();
	value = Value::INTEGER(0);
	const_cast<LogicalType &>(value.type()) = type;
}

void ValueWriter::SetBool(bool v) {
	D_ASSERT(value.type().id() == LogicalTypeId::BOOLEAN);
	SetNotNull();
	value.GetReferenceUnsafe<int8_t>() = v;
}

void ValueWriter::SetInt32(int32_t v) {
	SetNotNull();
	value.GetReferenceUnsafe<int32_t>() = v;
}

void ValueWriter::SetInt64(int64_t v) {
	SetNotNull();
	value.GetReferenceUnsafe<int64_t>() = v;
}

void ValueWriter::SetDouble(double v) {
	D_ASSERT(value.type().id() == LogicalTypeId::DOUBLE);
	SetNotNull();
	value.GetReferenceUnsafe<double>() = v;
}

void ValueWriter::SetNumeric(const hugeint_t &v) {
	D_ASSERT(value.type().id() == LogicalTypeId::DECIMAL);
	SetNotNull();
	value.GetReferenceUnsafe<hugeint_t>() = v;
}

void ValueWriter::SetInterval(const interval_t &v) {
	SetNotNull();
	value.GetReferenceUnsafe<interval_t>() = v;
}

void ValueWriter::SetString(string &&str) {
	D_ASSERT(value.type().InternalType() == PhysicalType::VARCHAR);
	SetNotNull();
	const_cast<string &>(StringValue::Get(value)) = move(str);
}

void ValueWriter::SetList() {
	D_ASSERT(value.type().id() == LogicalTypeId::LIST);
	SetNotNull();
}

ValueWriter ValueWriter::ListAppend() {
	D_ASSERT(value.type().id() == LogicalTypeId::LIST);
	auto &list = const_cast<vector<Value> &>(ListValue::GetChildren(value));
	auto &child_type = ListType::GetChildType(value.type());
	return ValueWriter(list.emplace_back(child_type));
}

void ValueWriter::SetStruct() {
	D_ASSERT(value.type().id() == LogicalTypeId::STRUCT);
	SetNotNull();
	auto &children = const_cast<vector<Value> &>(StructValue::GetChildren(value));
	for (auto &child_type : StructType::GetChildTypes(value.type())) {
		children.emplace_back(child_type.second);
	}
}

ValueWriter ValueWriter::operator[](idx_t index) {
	D_ASSERT(value.type().id() == LogicalTypeId::STRUCT && index < StructValue::GetChildren(value).size());
	return ValueWriter(const_cast<Value &>(StructValue::GetChildren(value)[index]));
}

void VectorExecuteUnary(DataChunk &args, Vector &result, value_unary_func func) {
	result.SetVectorType(args.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR ? VectorType::CONSTANT_VECTOR
	                                                                                 : VectorType::FLAT_VECTOR);
	for (idx_t i_row = 0; i_row < args.size(); ++i_row) {
		const Value value = args.GetValue(0, i_row);
		if (value.IsNull()) {
			result.SetValue(i_row, Value());
		} else {
			Value value_res(result.GetType());
			ValueWriter writer(value_res);
			result.SetValue(i_row, func(writer, ValueReader(value)) ? value_res : Value());
		}
	}
}

void VectorExecuteBinary(DataChunk &args, Vector &result, value_binary_func func) {
	bool is_constant = true;
	for (idx_t i = 0; i < args.ColumnCount(); ++i) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			is_constant = false;
			break;
		}
	}
	result.SetVectorType(is_constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	for (idx_t i_row = 0; i_row < args.size(); ++i_row) {
		const Value value1 = args.GetValue(0, i_row);
		const Value value2 = args.GetValue(1, i_row);
		if (value1.IsNull() || value2.IsNull()) {
			result.SetValue(i_row, Value());
		} else {
			Value value_res(result.GetType());
			ValueWriter writer(value_res);
			result.SetValue(i_row, func(writer, ValueReader(value1), ValueReader(value2)) ? value_res : Value());
		}
	}
}

} // namespace duckdb
