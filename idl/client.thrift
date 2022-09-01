include "base.thrift"

namespace cpp bytegraph
namespace py bytegraph
namespace go bytegraph
namespace java bytegraph

enum DirectionType {
  Forward = 1,
  Reverse = 2,
  Double = 3,
}

enum ErrorCode {
  SUCCESS = 0,
  SYSTEM_ERROR = 1,
  UNKNOWN_ERROR = 2,
  POINT_NOT_EXIST = 3,
  EDGE_NOT_EXIST = 4,
  RETRY = 5,
  TABLE_NOT_EXIST = 6,
  INVALID_REQUEST = 7,
  EDGE_ALREADY_EXIST = 8,
  NOT_IMPLEMENTED = 9,
  IO_TIMEOUT = 10,
  UDF_NOT_FOUND = 11,
  INDEX_OUT_OF_RANGE = 12,
  SERVICE_OVERLOAD = 13,
  EDGE_OVER_QUOTA = 14,
  PART_OVER_QUOTA = 15,
  SLAVE_WRITE_NOT_ALLOWED = 16,
  COMMIT_FAILED = 17,
  KEY_IN_BLACKLIST = 18,
  PSM_OVER_QUOTA = 19,
  PROPERTY_VALUE_INVALID = 20,
  PROPERTY_NOT_FOUND = 21,
  GREMLIN_INVALID_QUERY = 22,
  ELEM_NOT_EXIST = 23,
  WRITE_STALL = 24,
  TXN_CONFLICT = 25,
  REJECTED_BY_HLC = 26,
  AUTH_FAILED = 27,
  NOT_SETED = 255,
}

struct Value {
  1: optional bool bool_value;
  2: optional i32 int_value;
  3: optional i64 int64_value;
  4: optional double float_value;
  5: optional double double_value;
  6: optional binary string_value;
}

enum ClientProtocol{
    Binary = 0,
    ColumnarV1 = 1
}

struct GremlinQueryRequest {
  1: string table,
  2: string query,
  // if queries field is set, query/templates/parameters will be ignored.
  3: list<string> queries,
  // if templates field is set, parameters must be set, query/queries will be ignored.
  4: list<string> templates,
  5: list<map<string, Value>> parameters,
  6: list<map<string, binary>> binary_parameters,
  7: bool useBinary,
  8: bool compression,
  // ClientProtocol 用于标识客户端可解析的协议版本，用于db返回给客户端列式协议场景使用。需要先开启useBinary == true，该字段才可生效。
  9: ClientProtocol expect_protocol
  255: optional base.Base Base,
}

struct GremlinQueryResponse {
  1: ErrorCode errCode,
  2: string desc,
  // protobuf serialized object, only used in single query mode
  3: binary retPB,
  // protobuf serialized object, used in batch query mode
  4: list<binary> batchRet,
  5: list<string> batchDesc,
  6: list<ErrorCode> batchErrCode,
  // our own binary serialized object, used in batch query mode when config is enable
  7: list<binary> batchBinaryRet,
  8: optional list<string> txnIds,
  9: optional list<i64> txnTss,
  10: optional string txnId,
  11: optional i64 txnTs,
  12: optional list<i64> costs,
  255: optional base.BaseResp BaseResp,
}

service ByteGraphService {
    GremlinQueryResponse GremlinQuery(1: GremlinQueryRequest req),
}