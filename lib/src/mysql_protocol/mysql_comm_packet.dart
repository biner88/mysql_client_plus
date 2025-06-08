//mysql_comm_packet.dart
import 'dart:convert';
import 'dart:typed_data';
import 'package:buffer/buffer.dart' show ByteDataWriter;
import 'package:mysql_client_plus/exception.dart';
import 'package:mysql_client_plus/mysql_protocol.dart';
import 'package:mysql_client_plus/mysql_protocol_extension.dart';

/// Represents the COM_INIT_DB command in the MySQL protocol.
/// This command is used to select a database (schema) on the server.
class MySQLPacketCommInitDB extends MySQLPacketPayload {
  final String schemaName;

  MySQLPacketCommInitDB({
    required this.schemaName,
  });

  @override
  Uint8List encode() {
    final buffer = ByteDataWriter(endian: Endian.little);

    // command type
    buffer.writeUint8(2);
    buffer.write(utf8.encode(schemaName));

    return buffer.toBytes();
  }
}

/// Represents the COM_QUERY command in the MySQL protocol.
/// This command is used to send a SQL query to the server.
class MySQLPacketCommQuery extends MySQLPacketPayload {
  final String query;

  MySQLPacketCommQuery({
    required this.query,
  });

  @override
  Uint8List encode() {
    final buffer = ByteDataWriter(endian: Endian.little);
    buffer.writeUint8(3);
    buffer.write(utf8.encode(query));
    return buffer.toBytes();
  }
}

/// Represents the COM_STMT_PREPARE command in the MySQL protocol.
/// This command is used to prepare a SQL statement for execution.
class MySQLPacketCommStmtPrepare extends MySQLPacketPayload {
  final String query;

  MySQLPacketCommStmtPrepare({
    required this.query,
  });

  @override
  Uint8List encode() {
    final buffer = ByteDataWriter(endian: Endian.little);
    buffer.writeUint8(0x16);
    buffer.write(utf8.encode(query));
    return buffer.toBytes();
  }
}

/// Represents the COM_STMT_EXECUTE command in the MySQL protocol.
/// This command is used to execute a prepared statement with parameters.
class MySQLPacketCommStmtExecute extends MySQLPacketPayload {
  final int stmtID;

  final List<dynamic> params;

  final List<MySQLColumnType?> paramTypes;

  MySQLPacketCommStmtExecute({
    required this.stmtID,
    required this.params,
    required this.paramTypes,
  });
  @override
  Uint8List encode() {
    final buffer = ByteDataWriter(endian: Endian.little);

    // command type
    buffer.writeUint8(0x17);
    // stmt id
    buffer.writeUint32(stmtID, Endian.little);
    // flags
    buffer.writeUint8(0);
    // iteration count (always 1)
    buffer.writeUint32(1, Endian.little);

    // params
    if (params.isNotEmpty) {
      // create null-bitmap
      final bitmapSize = ((params.length + 7) ~/ 8);
      final nullBitmap = Uint8List(bitmapSize);

      // write null values into null bitmap
      for (int paramIndex = 0; paramIndex < params.length; paramIndex++) {
        if (params[paramIndex] == null) {
          final paramByteIndex = paramIndex ~/ 8;
          final paramBitIndex = paramIndex % 8;
          nullBitmap[paramByteIndex] |= (1 << paramBitIndex);
        }
      }
      // write null bitmap
      buffer.write(nullBitmap);

      // write new-param-bound flag
      buffer.writeUint8(1);

      // write not null values

      // write param types
      for (int i = 0; i < params.length; i++) {
        final paramType = paramTypes[i];
        if (paramType == null) {
          // If null, the type is mysqlColumnTypeNull = 0x06
          buffer.writeUint8(mysqlColumnTypeNull);
          buffer.writeUint8(0); // "unsigned" flag or other, usually 0
        } else {
          buffer.writeUint8(paramType.intVal);
          // For example, to indicate "unsigned", could set something. Here, 0 = no flag.
          buffer.writeUint8(0);
        }
      }

      // Write non-null parameter values
      for (int i = 0; i < params.length; i++) {
        final param = params[i];
        final paramType = paramTypes[i];
        if (param != null && paramType != null) {
          _writeParamValue(buffer, param, paramType);
        }
      }
    }

    return buffer.toBytes();
  }

  /// Writes a parameter value to the buffer based on its MySQL type.
  void _writeParamValue(
    ByteDataWriter buffer,
    dynamic param,
    MySQLColumnType type,
  ) {
    switch (type.intVal) {
      case mysqlColumnTypeTiny: // 1 byte
        // If parameter is boolean, convert to 1 or 0. Otherwise, assume 1-byte int.
        if (param is bool) {
          buffer.writeUint8(param ? 1 : 0);
        } else {
          // If param is int, convert to 8 bits (may overflow if >127).
          buffer.writeInt8(param);
        }
        break;

      case mysqlColumnTypeShort: // 2 bytes (int16)
        buffer.writeInt16(param, Endian.little);
        break;

      case mysqlColumnTypeLong: // 4 bytes (int32)
      case mysqlColumnTypeInt24: // in MySQL, 24 bits, but typically handled as 32 bits
        buffer.writeInt32(param, Endian.little);
        break;

      case mysqlColumnTypeLongLong: // 8 bytes (int64)
        buffer.writeInt64(param, Endian.little);
        break;

      case mysqlColumnTypeFloat: // 4 bytes float
        buffer.writeFloat32(param, Endian.little);
        break;

      case mysqlColumnTypeDouble: // 8 bytes double
        buffer.writeFloat64(param, Endian.little);
        break;

      case mysqlColumnTypeDate:
      case mysqlColumnTypeDateTime:
      case mysqlColumnTypeTimestamp:
        _writeDateTime(buffer, param);
        break;

      case mysqlColumnTypeTime:
        _writeTime(buffer, param);
        break;
      // Strings, BLOBs, DECIMALS etc. → length encoded + bytes
      case mysqlColumnTypeString:
      case mysqlColumnTypeVarString:
      case mysqlColumnTypeVarChar:
      case mysqlColumnTypeEnum:
      case mysqlColumnTypeSet:
      case mysqlColumnTypeLongBlob:
      case mysqlColumnTypeMediumBlob:
      case mysqlColumnTypeBlob:
      case mysqlColumnTypeTinyBlob:
      case mysqlColumnTypeGeometry:
      case mysqlColumnTypeBit:
      case mysqlColumnTypeDecimal:
      case mysqlColumnTypeNewDecimal:
        {
          // If parameter is Uint8List, send as binary; otherwise, convert to UTF-8 string
          final encodedData =
              (param is Uint8List) ? param : utf8.encode(param.toString());

          // First write the length (length-encoded)
          buffer.writeVariableEncInt(encodedData.length);
          // Then write the bytes
          buffer.write(encodedData);
        }
        break;
      case mysqlColumnTypeJson:
        String jsonString;
        if (param is String) {
          // Validate if it's valid JSON
          try {
            jsonDecode(param);
            jsonString = param;
          } catch (e) {
            // If not valid JSON, treat as normal string
            jsonString = jsonEncode(param);
          }
        } else if (param is Map || param is List) {
          jsonString = jsonEncode(param);
        } else {
          jsonString = jsonEncode(param.toString());
        }

        final encodedData = utf8.encode(jsonString);
        buffer.writeVariableEncInt(encodedData.length);
        buffer.write(encodedData);
        break;
      default:
        throw MySQLProtocolException(
          "Unsupported parameter type: ${type.intVal}",
        );
    }
  }

  /// Writes a DateTime value [dateTime] to the [buffer] according to the MySQL protocol.
  ///
  /// Depending on the values of year, month, day, hour, minute, second, and microsecond,
  /// the method chooses a format of 4, 7, or 11 bytes.
  void _writeDateTime(ByteDataWriter buffer, DateTime dateTime) {
    final year = dateTime.year;
    final month = dateTime.month;
    final day = dateTime.day;
    final hour = dateTime.hour;
    final minute = dateTime.minute;
    final second = dateTime.second;
    final microsecond = dateTime.microsecond;

    // If all values are zero, write 0 (no date/time data).
    if (year == 0 &&
        month == 0 &&
        day == 0 &&
        hour == 0 &&
        minute == 0 &&
        second == 0 &&
        microsecond == 0) {
      buffer.writeUint8(0);
      return;
    }

    if (microsecond > 0) {
      // 11 bytes: 1 for length, 2 for year, 1 for month, 1 for day,
      // 1 for hour, 1 for minute, 1 for second, 4 for microseconds
      buffer.writeUint8(11);
      buffer.writeUint16(year, Endian.little);
      buffer.writeUint8(month);
      buffer.writeUint8(day);
      buffer.writeUint8(hour);
      buffer.writeUint8(minute);
      buffer.writeUint8(second);
      buffer.writeUint32(microsecond, Endian.little);
    } else if (hour > 0 || minute > 0 || second > 0) {
      // 7 bytes: 1 for length, 2 for year, 1 for month, 1 for day,
      // 1 for hour, 1 for minute, 1 for second
      buffer.writeUint8(7);
      buffer.writeUint16(year, Endian.little);
      buffer.writeUint8(month);
      buffer.writeUint8(day);
      buffer.writeUint8(hour);
      buffer.writeUint8(minute);
      buffer.writeUint8(second);
    } else {
      // 4 bytes: 1 for length, 2 for year, 1 for month, 1 for day
      buffer.writeUint8(4);
      buffer.writeUint16(year, Endian.little);
      buffer.writeUint8(month);
      buffer.writeUint8(day);
    }
  }

  /// Writes a Time value (represented as DateTime) to the [buffer]
  /// according to the MySQL protocol.
  ///
  /// The MySQL binary protocol for TIME stores:
  /// - 1 byte for "length" (can be 0, 8, or 12).
  /// - 1 byte for sign (0=positive, 1=negative).
  /// - 4 bytes for "days".
  /// - 1 hour, 1 minute, 1 second [=3 bytes].
  /// - Optionally 4 bytes for microseconds, if present.
  ///
  /// Here, we interpret [time] as a DateTime where day/hour/minute/second represent
  /// only the time portion (e.g., 00:00 to 23:59:59).
  void _writeTime(ByteDataWriter buffer, DateTime time) {
    final hour = time.hour;
    final minute = time.minute;
    final second = time.second;
    final microsecond = time.microsecond;

    // If all zero, write 0 (time = 00:00:00).
    if (hour == 0 && minute == 0 && second == 0 && microsecond == 0) {
      buffer.writeUint8(0);
      return;
    }

    if (microsecond > 0) {
      // 12 bytes: 1 (len) + 1 (sign) + 4 (days=0) + 1 (hour) + 1 (min) + 1 (sec) + 4 (microsec)
      buffer.writeUint8(12);
      buffer.writeUint8(0); // sign = 0 (positive)
      buffer.writeUint32(0, Endian.little); // days = 0
      buffer.writeUint8(hour);
      buffer.writeUint8(minute);
      buffer.writeUint8(second);
      buffer.writeUint32(microsecond, Endian.little);
    } else {
      // 8 bytes: 1 (len) + 1 (sign) + 4 (days=0) + 1 (hour) + 1 (min) + 1 (sec)
      buffer.writeUint8(8);
      buffer.writeUint8(0); // sign = 0 (positive)
      buffer.writeUint32(0, Endian.little); // days = 0
      buffer.writeUint8(hour);
      buffer.writeUint8(minute);
      buffer.writeUint8(second);
    }
  }
}

/// Represents the COM_QUIT command in the MySQL protocol.
///
/// This command is used to close the connection with the server.
/// The packet consists of only a command byte (value 1).
class MySQLPacketCommQuit extends MySQLPacketPayload {
  @override
  Uint8List encode() {
    final buffer = ByteDataWriter(endian: Endian.little);
    // Write the QUIT command (1)
    buffer.writeUint8(1);
    return buffer.toBytes();
  }
}

/// Represents the COM_STMT_CLOSE command in the MySQL protocol.
///
/// This command is used to close a prepared statement and release
/// associated resources on the server. The packet contains:
/// - A command byte (value 0x19).
/// - The statement ID (stmtID) (4 bytes, little-endian).
class MySQLPacketCommStmtClose extends MySQLPacketPayload {
  /// ID of the statement to be closed.
  final int stmtID;

  /// Class constructor.
  MySQLPacketCommStmtClose({
    required this.stmtID,
  });

  @override
  Uint8List encode() {
    final buffer = ByteDataWriter(endian: Endian.little);
    // Write the COM_STMT_CLOSE command (0x19)
    buffer.writeUint8(0x19);
    // Write the statement ID (4 bytes, little-endian)
    buffer.writeUint32(stmtID, Endian.little);
    return buffer.toBytes();
  }
}
