#include <charconv>

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#endif
#include "fmt/format.h"
#include "json_common.hpp"

#include "variant-extension.hpp"
#include "variant.hpp"
#include "vector_proxy.hpp"
#include "converters.hpp"

namespace duckdb {

LogicalType getVariantType() {
	child_list_t<LogicalType> children;
	children.push_back(make_pair("__type", LogicalType::VARCHAR));
	children.push_back(make_pair("__value", LogicalType::JSON));
	auto variant_type = LogicalType::STRUCT(move(children));
	variant_type.SetAlias("VARIANT");

	return variant_type;
}

const LogicalType DDNumericType = LogicalType::DECIMAL(dd_numeric_width, dd_numeric_scale);

// clang-format off
// const LogicalType DDVariantType = LogicalType::STRUCT({
// 	{"__type", LogicalType::VARCHAR},
// 	{"__value", LogicalType::JSON}
// });
const LogicalType DDVariantType = getVariantType();
// clang-format on

namespace {

using namespace std::placeholders;

class VariantWriter {
public:
	VariantWriter(const LogicalType &arg_type, yyjson_mut_doc *doc = nullptr) : doc(doc), type(&arg_type) {
		is_list = type->id() == LogicalTypeId::LIST;
		if (is_list) {
			type = &ListType::GetChildType(*type);
		}
		switch (type->id()) {
		case LogicalTypeId::BOOLEAN:
			type_name = is_list ? "BOOL[]" : "BOOL";
			write_func = &VariantWriter::WriteBool;
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::HUGEINT:
			type_name = is_list ? "INT64[]" : "INT64";
			write_func = &VariantWriter::WriteInt64;
			break;
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			type_name = is_list ? "FLOAT64[]" : "FLOAT64";
			write_func = &VariantWriter::WriteFloat64;
			break;
		case LogicalTypeId::DECIMAL: {
			type_name = is_list ? "NUMERIC[]" : "NUMERIC";
			write_func = &VariantWriter::WriteNumeric;
			break;
		}
		case LogicalTypeId::VARCHAR:
			type_name = is_list ? "STRING[]" : "STRING";
			write_func = &VariantWriter::WriteString;
			break;
		case LogicalTypeId::BLOB:
			type_name = is_list ? "BYTES[]" : "BYTES";
			write_func = &VariantWriter::WriteBytes;
			break;
		case LogicalTypeId::UUID:
			type_name = is_list ? "STRING[]" : "STRING";
			write_func = &VariantWriter::WriteUUID;
			break;
		case LogicalTypeId::ENUM:
			type_name = is_list ? "STRING[]" : "STRING";
			write_func = &VariantWriter::WriteEnum;
			break;
		case LogicalTypeId::DATE:
			type_name = is_list ? "DATE[]" : "DATE";
			write_func = &VariantWriter::WriteDate;
			break;
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIME_TZ:
			type_name = is_list ? "TIME[]" : "TIME";
			write_func = &VariantWriter::WriteTime;
			break;
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_NS:
			type_name = is_list ? "DATETIME[]" : "DATETIME";
			write_func = &VariantWriter::WriteDatetime;
			break;
		case LogicalTypeId::TIMESTAMP_TZ:
			type_name = is_list ? "TIMESTAMP[]" : "TIMESTAMP";
			write_func = &VariantWriter::WriteTimestamp;
			break;
		case LogicalTypeId::INTERVAL:
			type_name = is_list ? "INTERVAL[]" : "INTERVAL";
			write_func = &VariantWriter::WriteInterval;
			break;
		case LogicalTypeId::SQLNULL:
			type_name = is_list ? "NULL[]" : "NULL";
			write_func = &VariantWriter::WriteNull;
			break;
		case LogicalTypeId::JSON:
			type_name = is_list ? "JSON[]" : "JSON";
			write_func = &VariantWriter::WriteJSON;
			break;
		case LogicalTypeId::LIST:
			type_name = is_list ? "JSON[]" : "JSON";
			write_func = &VariantWriter::WriteList;
			break;
		case LogicalTypeId::STRUCT:
			type_name = is_list ? "STRUCT[]" : "STRUCT";
			write_func = &VariantWriter::WriteStruct;
			break;
		case LogicalTypeId::MAP:
			type_name = is_list ? "STRUCT[]" : "STRUCT";
			write_func = &VariantWriter::WriteMap;
			break;
		default:
			type_name = "UNKNOWN";
			is_list = false;
			write_func = &VariantWriter::WriteNull;
			break;
		}
	}

	bool Process(ValueWriter &result, const ValueReader &arg) {
		auto p_doc = JSONCommon::CreateDocument();
		doc = *p_doc;
		yyjson_mut_val *root = ProcessValue(arg);
		if (yyjson_mut_is_null(root)) {
			return false;
		}
		result.SetStruct();
		result[0].SetString(type_name);
		yyjson_mut_doc_set_root(doc, root);
		size_t len;
		unique_ptr<char, decltype(&free)> data(yyjson_mut_write(doc, 0, &len), free);
		result[1].SetString(string(data.get(), len));
		return true;
	}

private:
	yyjson_mut_val *ProcessValue(const ValueReader &arg) {
		yyjson_mut_val *root;
		if (is_list) {
			root = yyjson_mut_arr(doc);
			for (ValueReader item : arg) {
				if (item.IsNull()) {
					yyjson_mut_arr_add_null(doc, root);
				} else {
					yyjson_mut_arr_append(root, (this->*write_func)(item));
				}
			}
		} else {
			root = (this->*write_func)(arg);
		}
		return root;
	}

	yyjson_mut_val *WriteNull(const ValueReader &arg) {
		return yyjson_mut_null(doc);
	}

	yyjson_mut_val *WriteBool(const ValueReader &arg) {
		return yyjson_mut_bool(doc, arg.GetBool());
	}

	yyjson_mut_val *WriteInt64(const ValueReader &arg) {
		auto val = arg.GetInt();
		return val ? yyjson_mut_int(doc, *val) : yyjson_mut_null(doc);
	}

	yyjson_mut_val *WriteFloat64(const ValueReader &arg) {
		double val = arg.GetReal();
		if (std::isinf(val)) {
			return yyjson_mut_str(doc, val < 0 ? "-Infinity" : "Infinity");
		}
		if (std::isnan(val)) {
			return yyjson_mut_str(doc, "NaN");
		}
		return yyjson_mut_real(doc, val);
	}

	yyjson_mut_val *WriteNumeric(const ValueReader &arg) {
		string val = arg.GetDecimal();
		return yyjson_mut_strncpy(doc, val.data(), val.size());
	}

	yyjson_mut_val *WriteString(const ValueReader &arg) {
		const string &val = arg.GetString();
		return yyjson_mut_strn(doc, val.data(), val.size());
	}

	yyjson_mut_val *WriteBytes(const ValueReader &arg) {
		string_t val(arg.GetString());
		idx_t size = Blob::ToBase64Size(val);
		string s(size, '\0');
		Blob::ToBase64(val, s.data());
		return yyjson_mut_strncpy(doc, s.data(), size);
	}

	yyjson_mut_val *WriteUUID(const ValueReader &arg) {
		string val = arg.GetUUID();
		return yyjson_mut_strncpy(doc, val.data(), val.size());
	}

	yyjson_mut_val *WriteEnum(const ValueReader &arg) {
		string val = arg.GetEnum();
		return yyjson_mut_strncpy(doc, val.data(), val.size());
	}

	yyjson_mut_val *WriteDate(const ValueReader &arg) {
		string val = arg.GetDate();
		return yyjson_mut_strncpy(doc, val.data(), val.size());
	}

	yyjson_mut_val *WriteTime(const ValueReader &arg) {
		string val = arg.GetTime();
		return yyjson_mut_strncpy(doc, val.data(), val.size());
	}

	yyjson_mut_val *WriteTimestamp(const ValueReader &arg) {
		string val = arg.GetTimestamp();
		return yyjson_mut_strncpy(doc, val.data(), val.size());
	}

	yyjson_mut_val *WriteDatetime(const ValueReader &arg) {
		string val = arg.GetDatetime();
		return yyjson_mut_strncpy(doc, val.data(), val.size());
	}

	yyjson_mut_val *WriteInterval(const ValueReader &arg) {
		const interval_t &val = arg.GetInterval();
		if (val.months < -10000 * 12 || val.months > 10000 * 12 || val.days < -3660000 || val.days > 3660000 ||
		    val.micros < -87840000 * Interval::MICROS_PER_HOUR || val.micros > 87840000 * Interval::MICROS_PER_HOUR) {
			return yyjson_mut_null(doc);
		}
		string s = IntervalToISOString(val);
		return yyjson_mut_strncpy(doc, s.data(), s.size());
	}

	yyjson_mut_val *WriteJSON(const ValueReader &arg) {
		auto arg_doc = JSONCommon::ReadDocument(arg.GetString());
		return yyjson_val_mut_copy(doc, yyjson_doc_get_root(*arg_doc));
	}

	yyjson_mut_val *WriteList(const ValueReader &arg) {
		yyjson_mut_val *obj = yyjson_mut_arr(doc);
		idx_t i = 0;
		for (ValueReader item : arg) {
			if (item.IsNull()) {
				yyjson_mut_arr_add_null(doc, obj);
			} else {
				yyjson_mut_arr_append(obj, VariantWriter(ListType::GetChildType(*type), doc).ProcessValue(item));
			}
		}
		return obj;
	}

	yyjson_mut_val *WriteStruct(const ValueReader &arg) {
		yyjson_mut_val *obj = yyjson_mut_obj(doc);
		idx_t i = 0;
		for (auto &[child_key, child_type] : StructType::GetChildTypes(*type)) {
			yyjson_mut_val *key = yyjson_mut_strn(doc, child_key.data(), child_key.size());
			ValueReader item = arg[i++];
			yyjson_mut_val *val =
			    item.IsNull() ? yyjson_mut_null(doc) : VariantWriter(child_type, doc).ProcessValue(item);
			yyjson_mut_obj_put(obj, key, val);
		}
		return obj;
	}

	yyjson_mut_val *WriteMap(const ValueReader &arg) {
		auto &arg_types = StructType::GetChildTypes(*type);
		if (ListType::GetChildType(arg_types[0].second).id() != LogicalTypeId::VARCHAR) {
			return yyjson_mut_null(doc);
		}
		VariantWriter writer = VariantWriter(ListType::GetChildType(arg_types[1].second), doc);
		yyjson_mut_val *obj = yyjson_mut_obj(doc);
		auto it_keys = arg[0].begin();
		auto it_values = arg[1].begin();
		size_t size = arg[0].ListSize();
		for (size_t i = 0; i < size; ++i) {
			const string &child_key = (*it_keys++).GetString();
			yyjson_mut_val *key = yyjson_mut_strn(doc, child_key.data(), child_key.size());
			ValueReader child_value = *it_values++;
			yyjson_mut_val *val = child_value.IsNull() ? yyjson_mut_null(doc) : writer.ProcessValue(child_value);
			yyjson_mut_obj_put(obj, key, val);
		}
		return obj;
	}

private:
	yyjson_mut_doc *doc;
	yyjson_mut_val *(VariantWriter::*write_func)(const ValueReader &) = nullptr;
	const char *type_name = nullptr;
	bool is_list = false;
	const LogicalType *type;
};

static void VariantFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType() == DDVariantType);
	VariantWriter writer(args.data[0].GetType());
	VectorExecuteUnary(args, result, std::bind(&VariantWriter::Process, &writer, _1, _2));
}

class VariantReaderBase {
public:
	bool ProcessScalar(ValueWriter &result, const ValueReader &arg) {
		auto doc = JSONCommon::ReadDocument(arg[1].GetString());
		auto val = yyjson_doc_get_root(*doc);
		return ReadScalar(result, val);
	}

	bool ProcessList(ValueWriter &result, const ValueReader &arg) {
		auto doc = JSONCommon::ReadDocument(arg[1].GetString());
		auto root = yyjson_doc_get_root(*doc);
		yyjson_arr_iter iter;
		if (!yyjson_arr_iter_init(root, &iter)) {
			return false;
		}
		result.SetList();
		yyjson_val *val;
		while (val = yyjson_arr_iter_next(&iter)) {
			ValueWriter item = result.ListAppend();
			if (!ReadScalar(item, val)) {
				item.SetNull();
			}
		}
		return true;
	}

	virtual bool ReadScalar(ValueWriter &result, yyjson_val *val) = 0;
};

class VariantReaderBool : public VariantReaderBase {
public:
	bool ReadScalar(ValueWriter &result, yyjson_val *val) override {
		if (!unsafe_yyjson_is_bool(val)) {
			return false;
		}
		result.SetBool(unsafe_yyjson_get_bool(val));
		return true;
	}
};

class VariantReaderInt64 : public VariantReaderBase {
public:
	bool ReadScalar(ValueWriter &result, yyjson_val *val) override {
		int64_t res;
		switch (unsafe_yyjson_get_tag(val)) {
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
			res = unsafe_yyjson_get_sint(val);
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT: {
			uint64_t i = unsafe_yyjson_get_uint(val);
			if (i > (uint64_t)std::numeric_limits<int64_t>::max()) {
				return false;
			}
			res = (int64_t)i;
			break;
		}
		default:
			return false;
		}
		result.SetInt64(res);
		return true;
	}
};

class VariantReaderFloat64 : public VariantReaderBase {
public:
	bool ReadScalar(ValueWriter &result, yyjson_val *val) override {
		double res;
		switch (unsafe_yyjson_get_tag(val)) {
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
			res = unsafe_yyjson_get_real(val);
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
			res = (double)unsafe_yyjson_get_uint(val);
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
			res = (double)unsafe_yyjson_get_sint(val);
			break;
		case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE: {
			const char *s = unsafe_yyjson_get_str(val);
			if (strcmp(s, "Infinity") == 0) {
				res = std::numeric_limits<double>::infinity();
			} else if (strcmp(s, "-Infinity") == 0) {
				res = -std::numeric_limits<double>::infinity();
			} else if (strcmp(s, "NaN") == 0) {
				res = NAN;
			} else {
				return false;
			}
			break;
		}
		default:
			return false;
		}
		result.SetDouble(res);
		return true;
	}
};

class VariantReaderNumeric : public VariantReaderBase {
public:
	bool ReadScalar(ValueWriter &result, yyjson_val *val) override {
		hugeint_t res;
		string message(1, ' ');
		switch (unsafe_yyjson_get_tag(val)) {
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
			if (!TryCastToDecimal::Operation(unsafe_yyjson_get_real(val), res, &message, dd_numeric_width,
			                                 dd_numeric_scale)) {
				return false;
			}
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
			if (!TryCastToDecimal::Operation(unsafe_yyjson_get_uint(val), res, &message, dd_numeric_width,
			                                 dd_numeric_scale)) {
				return false;
			}
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
			if (!TryCastToDecimal::Operation(unsafe_yyjson_get_sint(val), res, &message, dd_numeric_width,
			                                 dd_numeric_scale)) {
				return false;
			}
			break;
		case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
			if (!TryCastToDecimal::Operation(string_t(unsafe_yyjson_get_str(val)), res, &message, dd_numeric_width,
			                                 dd_numeric_scale)) {
				return false;
			}
			break;
		default:
			return false;
		}
		result.SetNumeric(res);
		return true;
	}
};

class VariantReaderString : public VariantReaderBase {
public:
	bool ProcessScalar(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return (tp == "STRING" || tp == "JSON") && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return (tp == "STRING[]" || tp == "JSON[]" || tp == "JSON") && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(ValueWriter &result, yyjson_val *val) override {
		const char *res = yyjson_get_str(val);
		if (!res) {
			return false;
		}
		result.SetString(res);
		return true;
	}
};

class VariantReaderBytes : public VariantReaderBase {
public:
	bool ProcessScalar(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return tp == "BYTES" && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return tp == "BYTES[]" && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(ValueWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		string_t str(str_val);
		idx_t size = Blob::FromBase64Size(str);
		string res(size, '\0');
		Blob::FromBase64(str, (data_ptr_t)res.data(), size);
		result.SetString(move(res));
		return true;
	}
};

class VariantReaderDate : public VariantReaderBase {
public:
	bool ProcessScalar(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return tp == "DATE" && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return tp == "DATE[]" && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(ValueWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		date_t res;
		idx_t pos;
		if (!Date::TryConvertDate(str_val, strlen(str_val), pos, res, true) ||
		    res.days == std::numeric_limits<int32_t>::max() || res.days <= -std::numeric_limits<int32_t>::max()) {
			return false;
		}
		result.SetInt32(res.days);
		return true;
	}
};

class VariantReaderTime : public VariantReaderBase {
public:
	bool ProcessScalar(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return tp == "TIME" && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return tp == "TIME[]" && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(ValueWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		dtime_t res;
		idx_t pos;
		if (!Time::TryConvertTime(str_val, strlen(str_val), pos, res, true)) {
			return false;
		}
		result.SetInt64(res.micros);
		return true;
	}
};

class VariantReaderTimestamp : public VariantReaderBase {
public:
	bool ProcessScalar(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return (tp == "TIMESTAMP" || tp == "DATE" || tp == "DATETIME") && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return (tp == "TIMESTAMP[]" || tp == "DATE[]" || tp == "DATETIME[]") &&
		       VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(ValueWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		timestamp_t res;
		if (!Timestamp::TryConvertTimestamp(str_val, strlen(str_val), res)) {
			return false;
		}
		result.SetInt64(res.value);
		return true;
	}
};

class VariantReaderDatetime : public VariantReaderBase {
public:
	bool ProcessScalar(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return (tp == "DATE" || tp == "DATETIME") && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return (tp == "DATE[]" || tp == "DATETIME[]") && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(ValueWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		timestamp_t res;
		if (!Timestamp::TryConvertTimestamp(str_val, strlen(str_val), res)) {
			return false;
		}
		result.SetInt64(res.value);
		return true;
	}
};

class VariantReaderInterval : public VariantReaderBase {
public:
	bool ProcessScalar(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return (tp == "INTERVAL" || tp == "TIME") && VariantReaderBase::ProcessScalar(result, arg);
	}

	bool ProcessList(ValueWriter &result, const ValueReader &arg) {
		const string &tp = arg[0].GetString();
		return (tp == "INTERVAL[]" || tp == "TIME[]") && VariantReaderBase::ProcessList(result, arg);
	}

	bool ReadScalar(ValueWriter &result, yyjson_val *val) override {
		const char *str_val = yyjson_get_str(val);
		if (!str_val) {
			return false;
		}
		interval_t res;
		if (!IntervalFromISOString(str_val, strlen(str_val), res)) {
			string message(1, ' ');
			if (!Interval::FromCString(str_val, strlen(str_val), res, &message, true)) {
				return false;
			}
		}
		result.SetInterval(res);
		return true;
	}
};

class VariantReaderJSON : public VariantReaderBase {
public:
	bool ProcessScalar(ValueWriter &result, const ValueReader &arg) {
		result.SetString(string(arg[1].GetString()));
		return true;
	}

	bool ReadScalar(ValueWriter &result, yyjson_val *val) override {
		auto res_doc = JSONCommon::CreateDocument();
		yyjson_mut_doc_set_root(*res_doc, yyjson_val_mut_copy(*res_doc, val));
		size_t len;
		unique_ptr<char, decltype(&free)> data(yyjson_mut_write(*res_doc, 0, &len), free);
		result.SetString(string(data.get(), len));
		return true;
	}
};

template <class Reader>
static void FromVariantFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType() == DDVariantType);
	Reader reader;
	VectorExecuteUnary(args, result, std::bind(&Reader::ProcessScalar, &reader, _1, _2));
}

template <class Reader>
static void FromVariantListFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType() == DDVariantType);
	Reader reader;
	VectorExecuteUnary(args, result, std::bind(&Reader::ProcessList, &reader, _1, _2));
}

static bool VariantAccessWrite(ValueWriter &result, const ValueReader &arg, yyjson_val *val) {
	if (!val || unsafe_yyjson_is_null(val)) {
		return false;
	}
	const string &arg_type = arg[0].GetString();
	string res_type;
	if (StringUtil::StartsWith(arg_type, "JSON") || StringUtil::StartsWith(arg_type, "STRUCT")) {
		switch (unsafe_yyjson_get_tag(val)) {
		case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
			res_type = "STRING";
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
			res_type = "FLOAT64";
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
			res_type = "INT64";
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
			res_type = yyjson_get_uint(val) > (uint64_t)std::numeric_limits<int64_t>::max() ? "FLOAT64" : "INT64";
			break;
		case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
		case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
			res_type = "BOOL";
			break;
		default:
			res_type = "JSON";
			break;
		}
	} else {
		res_type = arg_type.substr(0, arg_type.find('['));
	}

	result.SetStruct();
	result[0].SetString(move(res_type));
	auto res_doc = JSONCommon::CreateDocument();
	yyjson_mut_doc_set_root(*res_doc, yyjson_val_mut_copy(*res_doc, val));
	size_t len;
	unique_ptr<char, decltype(&free)> data(yyjson_mut_write(*res_doc, 0, &len), free);
	result[1].SetString(string(data.get(), len));
	return true;
}

static bool VariantAccessIndexImpl(ValueWriter &result, const ValueReader &arg, const ValueReader &index) {
	auto idx = index.GetUInt();
	if (!idx) {
		return false;
	}
	auto arg_doc = JSONCommon::ReadDocument(arg[1].GetString());
	auto arg_root = yyjson_doc_get_root(*arg_doc);
	return VariantAccessWrite(result, arg, yyjson_arr_get(arg_root, *idx));
}

static bool VariantAccessKeyImpl(ValueWriter &result, const ValueReader &arg, const ValueReader &index) {
	string key = index.GetString();
	auto arg_doc = JSONCommon::ReadDocument(arg[1].GetString());
	auto arg_root = yyjson_doc_get_root(*arg_doc);
	return VariantAccessWrite(result, arg, yyjson_obj_get(arg_root, key.data()));
}

static void VariantAccessIndexFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType() == DDVariantType);
	VectorExecuteBinary(args, result, VariantAccessIndexImpl);
}

static void VariantAccessKeyFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType() == DDVariantType);
	VectorExecuteBinary(args, result, VariantAccessKeyImpl);
}

static string VariantSortHashReal(std::string_view arg) {
	bool negative = arg[0] == '-';
	string res(4, '\0');
	res[0] = '2' - negative;
	int start = -1, pos_d = arg.size(), exp = 0;
	for (size_t i = negative; i < arg.size(); ++i) {
		char c = arg[i];
		if (c == '.') {
			pos_d = i;
			continue;
		}
		if (c == 'e' || c == 'E') {
			if (pos_d > i) {
				pos_d = i;
			}
			exp = atoi(&arg[i + 1]);
			break;
		}
		if (start < 0) {
			if (c == '0') {
				continue;
			}
			start = i;
		}
		res += negative ? '0' + '9' - c : c;
	}
	if (start < 0) {
		return "2";
	}
	exp += pos_d - start - (pos_d > start);
	char filler;
	if (negative) {
		filler = '9';
		exp = 500 - exp;
	} else {
		filler = '0';
		exp += 500;
	}
	std::to_chars(&res[1], &res[4], exp);
	res.append(77 - res.size() + 4, filler);
	return res;
}

static string VariantSortHashInt(const string &arg) {
	string res;
	if (arg == "0") {
		res = '2';
	} else if (arg[0] == '-') {
		res = '1' + to_string(502 - arg.size());
		for (size_t i = 1; i < arg.size(); ++i) {
			res += '0' + '9' - arg[i];
		}
		res.append(77 - arg.size() + 1, '9');
	} else {
		res = '2' + to_string(499 + arg.size());
		res.append(arg);
		res.append(77 - arg.size(), '0');
	}
	return res;
}

static bool VariantSortHashImpl(ValueWriter &writer, const ValueReader &arg, const ValueReader &case_sensitive) {
	auto doc = JSONCommon::ReadDocument(arg[1].GetString());
	auto val = yyjson_doc_get_root(*doc);
	if (!val || unsafe_yyjson_is_null(val)) {
		return false;
	}
	string result;
	const string &tp = arg[0].GetString();
	bool is_json = tp == "JSON";
	auto js_tp = unsafe_yyjson_get_type(val);
	auto js_tag = unsafe_yyjson_get_tag(val);
	if (tp == "BOOL" || is_json && js_tp == YYJSON_TYPE_BOOL) {
		result = unsafe_yyjson_get_bool(val) ? "01" : "00";
	} else if (tp == "FLOAT64" || is_json && js_tag == (YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL)) {
		switch (js_tag) {
		case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
			if (string s = unsafe_yyjson_get_str(val); s == "NaN") {
				result = '1';
			} else if (s == "-Infinity") {
				result = "10";
			} else if (s == "Infinity") {
				result = "29";
			} else {
				return false;
			}
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
			if (double v = unsafe_yyjson_get_real(val); v == 0.0) {
				result = '2';
			} else {
				result = VariantSortHashReal(duckdb_fmt::format("{:.16e}", v));
			}
			break;
		default:
			return false;
		}
	} else if (tp == "INT64" || is_json && js_tp == YYJSON_TYPE_NUM) {
		switch (js_tag) {
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
			result = VariantSortHashInt(to_string(unsafe_yyjson_get_sint(val)));
			break;
		case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
			result = VariantSortHashInt(to_string(unsafe_yyjson_get_uint(val)));
			break;
		default:
			return false;
		}
	} else if (tp == "NUMERIC") {
		D_ASSERT(js_tp == YYJSON_TYPE_STR);
		result = VariantSortHashReal(unsafe_yyjson_get_str(val));
	} else if (tp == "STRING" || is_json && js_tp == YYJSON_TYPE_STR) {
		result = string("3") + unsafe_yyjson_get_str(val);
		if (!case_sensitive.GetBool()) {
			std::transform(result.begin(), result.end(), result.begin(),
			               [](unsigned char c) { return std::tolower(c); });
		}
	} else if (tp == "BYTES") {
		if (const char *s = yyjson_get_str(val)) {
			idx_t size = Blob::FromBase64Size(s);
			string decoded(size, '\0');
			Blob::FromBase64(s, (data_ptr_t)decoded.data(), size);
			result = '4';
			for (unsigned cp : decoded) {
				if (cp == 0) {
					cp = 256;
				}
				if (cp <= 0x7F) {
					result += cp;
				} else {
					result += (cp >> 6) + 192;
					result += (cp & 63) + 128;
				}
			}
		} else {
			return false;
		}
	} else if (tp == "TIME") {
		result = string("5") + unsafe_yyjson_get_str(val);
	} else if (tp == "DATE") {
		result = string("6") + unsafe_yyjson_get_str(val) + "T00:00:00";
	} else if (tp == "DATETIME") {
		result = string("6") + unsafe_yyjson_get_str(val);
	} else if (tp == "TIMESTAMP") {
		result = string("6") + unsafe_yyjson_get_str(val);
	} else if (tp == "INTERVAL") {
		const char *str_val = yyjson_get_str(val);
		interval_t iv;
		if (!IntervalFromISOString(str_val, strlen(str_val), iv)) {
			return false;
		}
		int64_t micros = Interval::GetMicro(iv);
		result = duckdb_fmt::format("7{:019d}000", micros + 943488000000000000);
	} else {
		auto res_doc = JSONCommon::CreateDocument();
		yyjson_mut_doc_set_root(*res_doc, yyjson_val_mut_copy(*res_doc, val));
		size_t len;
		unique_ptr<char, decltype(&free)> data(yyjson_mut_write(*res_doc, 0, &len), free);
		result = '9';
		if (!case_sensitive.GetBool() &&
		    (tp == "STRING[]" || StringUtil::StartsWith(tp, "STRUCT") || StringUtil::StartsWith(tp, "JSON"))) {
			std::transform(data.get(), data.get() + len, data.get(), [](unsigned char c) { return std::tolower(c); });
		}
		result.append(data.get(), len);
	}
	writer.SetString(move(result));
	return true;
}

static bool VariantFromSortHashNumber(ValueWriter &writer, bool negative, int ex, std::string_view digits,
                                      bool int_range) {
	if (digits.size() <= ex + 1 && int_range) {
		uint64_t res;
		std::from_chars(digits.data(), &digits[digits.size()], res);
		for (size_t i = ex + 1 - digits.size(); i-- > 0;) {
			res *= 10;
		}
		return VariantWriter(LogicalType::BIGINT).Process(writer, ValueReader(Value::BIGINT(negative ? 0 - res : res)));
	}
	string s;
	if (negative) {
		s += '-';
	}
	s += digits[0];
	s += '.';
	s.append(digits, 1);
	if (digits.size() < 17) {
		s.append(17 - digits.size(), '0');
	}
	s += duckdb_fmt::format("e{:+03d}", ex);
	double d = stod(s);
	if (duckdb_fmt::format("{:.16e}", d) == s) {
		return VariantWriter(LogicalType::DOUBLE).Process(writer, ValueReader(d));
	}
	Value v = s;
	try {
		if (!v.DefaultTryCastAs(DDNumericType)) {
			return false;
		}
	} catch (OutOfRangeException) {
		return false;
	}
	return VariantWriter(DDNumericType).Process(writer, ValueReader(v));
}

static bool VariantFromSortHashImpl(ValueWriter &writer, const ValueReader &reader) {
	const string &arg = reader.GetString();
	switch (arg[0]) {
	case '0': {
		bool res = arg[1] == '1';
		return VariantWriter(LogicalType::BOOLEAN).Process(writer, ValueReader(Value::BOOLEAN(res)));
	}
	case '1': {
		double res;
		if (arg.size() == 1) {
			res = NAN;
		} else if (arg.size() == 2 && arg[1] == '0') {
			res = -std::numeric_limits<double>::infinity();
		} else {
			const char *start = &arg[4], *end = &arg.back();
			while (end >= start && *end == '9') {
				--end;
			}
			string s;
			s.reserve(end - start + 1);
			while (start <= end) {
				s += '0' + '9' - *start++;
			}
			int ex;
			std::from_chars(&arg[1], &arg[4], ex);
			return VariantFromSortHashNumber(writer, true, 500 - ex, s,
			                                 arg >= "14820776627963145224191" && arg <= "15009");
		}
		return VariantWriter(LogicalType::DOUBLE).Process(writer, ValueReader(res));
	}
	case '2': {
		if (arg.size() == 1) {
			return VariantWriter(LogicalType::INTEGER).Process(writer, ValueReader(0));
		} else if (arg.size() == 2 && arg[1] == '9') {
			return VariantWriter(LogicalType::DOUBLE)
			    .Process(writer, ValueReader(std::numeric_limits<double>::infinity()));
		}
		std::string_view s(&arg[4], arg.size() - 4);
		s.remove_suffix(s.size() - 1 - s.find_last_not_of('0'));
		int ex;
		std::from_chars(&arg[1], &arg[4], ex);
		return VariantFromSortHashNumber(writer, false, ex - 500, s,
		                                 arg >= "25001" && arg <= "251892233720368547758071");
	}
	case '3':
		return VariantWriter(LogicalType::VARCHAR).Process(writer, ValueReader(arg.substr(1)));
	case '4': {
		string decoded;
		for (size_t i = 1; i < arg.size(); ++i) {
			unsigned c = (unsigned char)arg[i];
			if (c <= 127) {
				decoded += c;
			} else {
				D_ASSERT(c >= 192 && c <= 196);
				c = ((c - 192) << 6) + ((unsigned char)arg[++i] - 128);
				if (c == 256) {
					c = 0;
				}
				decoded += c;
			}
		}
		return VariantWriter(LogicalType::BLOB).Process(writer, ValueReader(Value::BLOB_RAW(decoded)));
	}
	case '5':
		return VariantWriter(LogicalType::TIME)
		    .Process(writer, ValueReader(Value(arg.substr(1)).DefaultCastAs(LogicalType::TIME)));
	case '6':
		if (StringUtil::EndsWith(arg, "T00:00:00")) {
			return VariantWriter(LogicalType::DATE)
			    .Process(writer, ValueReader(Value(arg.substr(1)).DefaultCastAs(LogicalType::DATE)));
		} else {
			return VariantWriter(LogicalType::TIMESTAMP)
			    .Process(writer, ValueReader(Value(arg.substr(1)).DefaultCastAs(LogicalType::TIMESTAMP)));
		}
	case '7': {
		int64_t micros;
		std::from_chars(&arg[1], &arg[arg.size() - 3], micros);
		micros -= 943488000000000000;
		return VariantWriter(LogicalType::INTERVAL)
		    .Process(writer, ValueReader(Value::INTERVAL(Interval::FromMicro(micros))));
	}
	case '9': {
		return VariantWriter(LogicalType::JSON).Process(writer, ValueReader(Value::JSON(arg.substr(1))));
	}
	default:
		return false;
	}
}

static void VariantSortHash(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType() == DDVariantType);
	VectorExecuteBinary(args, result, VariantSortHashImpl);
}

static void VariantFromSortHash(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[0].GetType().id() == LogicalTypeId::VARCHAR);
	VectorExecuteUnary(args, result, VariantFromSortHashImpl);
}

} // namespace

#define REGISTER_FUNCTION(TYPE, SQL_NAME, C_NAME)                                                                      \
	CreateScalarFunctionInfo from_variant_##SQL_NAME##_info(                                                           \
	    ScalarFunction("from_variant_" #SQL_NAME, {DDVariantType}, TYPE, FromVariantFunc<VariantReader##C_NAME>));     \
	catalog.CreateFunction(context, &from_variant_##SQL_NAME##_info);                                                  \
	CreateScalarFunctionInfo from_variant_##SQL_NAME##_array_info(                                                     \
	    ScalarFunction("from_variant_" #SQL_NAME "_array", {DDVariantType}, LogicalType::LIST(TYPE),                   \
	                   FromVariantListFunc<VariantReader##C_NAME>));                                                   \
	catalog.CreateFunction(context, &from_variant_##SQL_NAME##_array_info);

void VariantExtension::LoadVariant(Connection &con) {
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	// add the "variant" type
	CreateTypeInfo variant_type_info("VARIANT", DDVariantType);
	variant_type_info.temporary = true;
	variant_type_info.internal = true;
	catalog.CreateType(*con.context, &variant_type_info);

	CreateScalarFunctionInfo variant_info(
	    ScalarFunction("variant", {LogicalType::ANY}, DDVariantType, VariantFunction));
	catalog.CreateFunction(context, &variant_info);

	REGISTER_FUNCTION(LogicalType::BOOLEAN, bool, Bool)
	REGISTER_FUNCTION(LogicalType::BIGINT, int64, Int64)
	REGISTER_FUNCTION(LogicalType::DOUBLE, float64, Float64)
	REGISTER_FUNCTION(DDNumericType, numeric, Numeric)
	REGISTER_FUNCTION(LogicalType::VARCHAR, string, String)
	REGISTER_FUNCTION(LogicalType::BLOB, bytes, Bytes)
	REGISTER_FUNCTION(LogicalType::DATE, date, Date)
	REGISTER_FUNCTION(LogicalType::TIME, time, Time)
	REGISTER_FUNCTION(LogicalType::TIMESTAMP_TZ, timestamp, Timestamp)
	REGISTER_FUNCTION(LogicalType::TIMESTAMP, datetime, Datetime)
	REGISTER_FUNCTION(LogicalType::INTERVAL, interval, Interval)
	REGISTER_FUNCTION(LogicalType::JSON, json, JSON)

	ScalarFunctionSet variant_access_set("variant_access");
	variant_access_set.AddFunction(
	    ScalarFunction({DDVariantType, LogicalType::BIGINT}, DDVariantType, VariantAccessIndexFunc));
	variant_access_set.AddFunction(
	    ScalarFunction({DDVariantType, LogicalType::VARCHAR}, DDVariantType, VariantAccessKeyFunc));
	CreateScalarFunctionInfo variant_access_info(move(variant_access_set));
	catalog.CreateFunction(context, &variant_access_info);

	CreateScalarFunctionInfo sort_hash_info(ScalarFunction("variant_sort_hash", {DDVariantType, LogicalType::BOOLEAN},
	                                                       LogicalType::VARCHAR, VariantSortHash));
	catalog.CreateFunction(context, &sort_hash_info);

	CreateScalarFunctionInfo from_sort_hash_info(
	    ScalarFunction("variant_from_sort_hash", {LogicalType::VARCHAR}, DDVariantType, VariantFromSortHash));
	catalog.CreateFunction(context, &from_sort_hash_info);
}

} // namespace duckdb
