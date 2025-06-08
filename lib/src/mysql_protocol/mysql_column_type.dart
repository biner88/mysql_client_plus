import 'dart:convert';
import 'dart:typed_data';
import 'package:tuple/tuple.dart';
import 'package:mysql_client_plus/exception.dart';
import 'package:mysql_client_plus/mysql_protocol_extension.dart';

const mysqlColumnTypeDecimal = 0x00;
const mysqlColumnTypeTiny = 0x01;
const mysqlColumnTypeShort = 0x02;
const mysqlColumnTypeLong = 0x03;
const mysqlColumnTypeFloat = 0x04;
const mysqlColumnTypeDouble = 0x05;
const mysqlColumnTypeNull = 0x06;
const mysqlColumnTypeTimestamp = 0x07;
const mysqlColumnTypeLongLong = 0x08;
const mysqlColumnTypeInt24 = 0x09;
const mysqlColumnTypeDate = 0x0a;
const mysqlColumnTypeTime = 0x0b;
const mysqlColumnTypeDateTime = 0x0c;
const mysqlColumnTypeYear = 0x0d;
const mysqlColumnTypeNewDate = 0x0e;
const mysqlColumnTypeVarChar = 0x0f;
const mysqlColumnTypeBit = 0x10;
const mysqlColumnTypeTimestamp2 = 0x11;
const mysqlColumnTypeDateTime2 = 0x12;
const mysqlColumnTypeTime2 = 0x13;
const mysqlColumnTypeJson = 0xf5;
const mysqlColumnTypeNewDecimal = 0xf6;
const mysqlColumnTypeEnum = 0xf7;
const mysqlColumnTypeSet = 0xf8;
const mysqlColumnTypeTinyBlob = 0xf9;
const mysqlColumnTypeMediumBlob = 0xfa;
const mysqlColumnTypeLongBlob = 0xfb;
const mysqlColumnTypeBlob = 0xfc;
const mysqlColumnTypeVarString = 0xfd;
const mysqlColumnTypeString = 0xfe;
const mysqlColumnTypeGeometry = 0xff;

class MySQLColumnType {
  final int _value;

  const MySQLColumnType._(int value) : _value = value;
  factory MySQLColumnType.create(int value) => MySQLColumnType._(value);
  int get intVal => _value;

  static const decimalType = MySQLColumnType._(mysqlColumnTypeDecimal);
  static const tinyType = MySQLColumnType._(mysqlColumnTypeTiny);
  static const shortType = MySQLColumnType._(mysqlColumnTypeShort);
  static const longType = MySQLColumnType._(mysqlColumnTypeLong);
  static const floatType = MySQLColumnType._(mysqlColumnTypeFloat);
  static const doubleType = MySQLColumnType._(mysqlColumnTypeDouble);
  static const nullType = MySQLColumnType._(mysqlColumnTypeNull);
  static const timestampType = MySQLColumnType._(mysqlColumnTypeTimestamp);
  static const longLongType = MySQLColumnType._(mysqlColumnTypeLongLong);
  static const int24Type = MySQLColumnType._(mysqlColumnTypeInt24);
  static const dateType = MySQLColumnType._(mysqlColumnTypeDate);
  static const timeType = MySQLColumnType._(mysqlColumnTypeTime);
  static const dateTimeType = MySQLColumnType._(mysqlColumnTypeDateTime);
  static const yearType = MySQLColumnType._(mysqlColumnTypeYear);
  static const newDateType = MySQLColumnType._(mysqlColumnTypeNewDate);
  static const varChartType = MySQLColumnType._(mysqlColumnTypeVarChar);
  static const bitType = MySQLColumnType._(mysqlColumnTypeBit);
  static const timestamp2Type = MySQLColumnType._(mysqlColumnTypeTimestamp2);
  static const dateTime2Type = MySQLColumnType._(mysqlColumnTypeDateTime2);
  static const time2Type = MySQLColumnType._(mysqlColumnTypeTime2);
  static const jsonType = MySQLColumnType._(mysqlColumnTypeJson);
  static const newDecimalType = MySQLColumnType._(mysqlColumnTypeNewDecimal);
  static const enumType = MySQLColumnType._(mysqlColumnTypeEnum);
  static const setType = MySQLColumnType._(mysqlColumnTypeSet);
  static const tinyBlobType = MySQLColumnType._(mysqlColumnTypeTinyBlob);
  static const mediumBlobType = MySQLColumnType._(mysqlColumnTypeMediumBlob);
  static const longBlobType = MySQLColumnType._(mysqlColumnTypeLongBlob);
  static const blobType = MySQLColumnType._(mysqlColumnTypeBlob);
  static const varStringType = MySQLColumnType._(mysqlColumnTypeVarString);
  static const stringType = MySQLColumnType._(mysqlColumnTypeString);
  static const geometryType = MySQLColumnType._(mysqlColumnTypeGeometry);

  T? convertStringValueToProvidedType<T>(dynamic value, [int? columnLength]) {
    if (value == null) {
      return null;
    }

    if (T == Uint8List && value is Uint8List) {
      return value as T;
    }

    if (T == String || T == dynamic) {
      return value as T;
    }

    if (T == bool) {
      if (_value == mysqlColumnTypeTiny && columnLength == 1) {
        return int.parse(value) > 0 as T;
      } else {
        throw MySQLProtocolException(
          "Cannot convert MySQL type $_value to requested type bool",
        );
      }
    }

    if (T == int) {
      switch (_value) {
        case mysqlColumnTypeTiny:
        case mysqlColumnTypeShort:
        case mysqlColumnTypeLong:
        case mysqlColumnTypeLongLong:
        case mysqlColumnTypeInt24:
        case mysqlColumnTypeYear:
          return int.parse(value) as T;
        default:
          throw MySQLProtocolException(
            "Cannot convert MySQL type $_value to requested type int",
          );
      }
    }

    if (T == double) {
      switch (_value) {
        case mysqlColumnTypeTiny:
        case mysqlColumnTypeShort:
        case mysqlColumnTypeLong:
        case mysqlColumnTypeLongLong:
        case mysqlColumnTypeInt24:
        case mysqlColumnTypeFloat:
        case mysqlColumnTypeDouble:
          return double.parse(value) as T;
        default:
          throw MySQLProtocolException(
            "Cannot convert MySQL type $_value to requested type double",
          );
      }
    }

    if (T == num) {
      switch (_value) {
        case mysqlColumnTypeTiny:
        case mysqlColumnTypeShort:
        case mysqlColumnTypeLong:
        case mysqlColumnTypeLongLong:
        case mysqlColumnTypeInt24:
        case mysqlColumnTypeFloat:
        case mysqlColumnTypeDouble:
          return num.parse(value) as T;
        default:
          throw MySQLProtocolException(
            "Cannot convert MySQL type $_value to requested type num",
          );
      }
    }

    if (T == DateTime) {
      switch (_value) {
        case mysqlColumnTypeDate:
        case mysqlColumnTypeDateTime2:
        case mysqlColumnTypeDateTime:
        case mysqlColumnTypeTimestamp:
        case mysqlColumnTypeTimestamp2:
          return DateTime.parse(value) as T;
        default:
          throw MySQLProtocolException(
            "Cannot convert MySQL type $_value to requested type DateTime",
          );
      }
    }
    if (T == Map || T == List) {
      switch (_value) {
        case mysqlColumnTypeJson:
          if (value is String) {
            final decoded = jsonDecode(value);
            if (T == dynamic || decoded is T) {
              return decoded as T;
            }
          }
          if (value is Map && T == Map) {
            return value as T;
          }
          if (value is List && T == List) {
            return value as T;
          }
          throw MySQLClientException('Cannot convert JSON to $T');
        default:
          throw MySQLProtocolException(
            "Cannot convert MySQL type $_value to requested type JSON",
          );
      }
    }
    throw MySQLProtocolException(
      "Cannot convert MySQL type $_value to requested type ${T.runtimeType}",
    );
  }

  Type getBestMatchDartType(int columnLength) {
    switch (_value) {
      case mysqlColumnTypeString:
      case mysqlColumnTypeVarString:
      case mysqlColumnTypeVarChar:
      case mysqlColumnTypeEnum:
      case mysqlColumnTypeSet:
      case mysqlColumnTypeJson:
        return String;
      case mysqlColumnTypeLongBlob:
      case mysqlColumnTypeMediumBlob:
      case mysqlColumnTypeBlob:
      case mysqlColumnTypeTinyBlob:
        return Uint8List;
      case mysqlColumnTypeGeometry:
      case mysqlColumnTypeBit:
      case mysqlColumnTypeDecimal:
      case mysqlColumnTypeNewDecimal:
        return String;
      case mysqlColumnTypeTiny:
        if (columnLength == 1) {
          return bool;
        } else {
          return int;
        }
      case mysqlColumnTypeShort:
      case mysqlColumnTypeLong:
      case mysqlColumnTypeLongLong:
      case mysqlColumnTypeInt24:
      case mysqlColumnTypeYear:
        return int;
      case mysqlColumnTypeFloat:
      case mysqlColumnTypeDouble:
        return double;
      case mysqlColumnTypeDate:
      case mysqlColumnTypeDateTime2:
      case mysqlColumnTypeDateTime:
      case mysqlColumnTypeTimestamp:
      case mysqlColumnTypeTimestamp2:
        return DateTime;
      default:
        return String;
    }
  }
}

Tuple2<dynamic, int> parseBinaryColumnData(
  int columnType,
  ByteData data,
  Uint8List buffer,
  int startOffset,
) {
  switch (columnType) {
    case mysqlColumnTypeTiny:
      {
        final value = data.getInt8(startOffset);
        return Tuple2(value.toString(), 1);
      }

    case mysqlColumnTypeShort:
      {
        final value = data.getInt16(startOffset, Endian.little);
        return Tuple2(value.toString(), 2);
      }

    case mysqlColumnTypeLong:
    case mysqlColumnTypeInt24:
      {
        final value = data.getInt32(startOffset, Endian.little);
        return Tuple2(value.toString(), 4);
      }

    case mysqlColumnTypeLongLong:
      {
        final value = data.getInt64(startOffset, Endian.little);
        return Tuple2(value.toString(), 8);
      }

    case mysqlColumnTypeFloat:
      {
        final value = data.getFloat32(startOffset, Endian.little);
        return Tuple2(value.toString(), 4);
      }

    case mysqlColumnTypeDouble:
      {
        final value = data.getFloat64(startOffset, Endian.little);
        return Tuple2(value.toString(), 8);
      }

    case mysqlColumnTypeDate:
    case mysqlColumnTypeDateTime:
    case mysqlColumnTypeTimestamp:
      {
        final initialOffset = startOffset;
        final numOfBytes = data.getUint8(startOffset);
        startOffset += 1;

        // Quando numOfBytes == 0, MySQL envia datas/timestamps '0000-00-00 00:00:00'
        if (numOfBytes == 0) {
          return Tuple2("0000-00-00 00:00:00", 1);
        }

        var year = 0, month = 0, day = 0;
        var hour = 0, minute = 0, second = 0, microSecond = 0;

        if (numOfBytes >= 4) {
          year = data.getUint16(startOffset, Endian.little);
          startOffset += 2;
          month = data.getUint8(startOffset);
          startOffset += 1;
          day = data.getUint8(startOffset);
          startOffset += 1;
        }
        if (numOfBytes >= 7) {
          hour = data.getUint8(startOffset);
          startOffset += 1;
          minute = data.getUint8(startOffset);
          startOffset += 1;
          second = data.getUint8(startOffset);
          startOffset += 1;
        }
        if (numOfBytes >= 11) {
          microSecond = data.getUint32(startOffset, Endian.little);
          startOffset += 4;
        }

        final result = StringBuffer()
          ..write('$year-')
          ..write('${month.toString().padLeft(2, '0')}-')
          ..write('${day.toString().padLeft(2, '0')} ')
          ..write('${hour.toString().padLeft(2, '0')}:')
          ..write('${minute.toString().padLeft(2, '0')}:')
          ..write(second.toString().padLeft(2, '0'));

        if (numOfBytes >= 11) {
          result.write('.$microSecond');
        }

        final consumed = startOffset - initialOffset;
        return Tuple2(result.toString(), consumed);
      }

    case mysqlColumnTypeTime:
    case mysqlColumnTypeTime2:
      {
        final initialOffset = startOffset;
        final numOfBytes = data.getUint8(startOffset);
        startOffset += 1;

        if (numOfBytes == 0) {
          return Tuple2("00:00:00", 1);
        }

        var isNegative = false;
        var days = 0, hours = 0, minutes = 0, seconds = 0, microSecond = 0;

        if (numOfBytes >= 8) {
          isNegative = data.getUint8(startOffset) > 0;
          startOffset += 1;
          days = data.getUint32(startOffset, Endian.little);
          startOffset += 4;
          hours = data.getUint8(startOffset);
          startOffset += 1;
          minutes = data.getUint8(startOffset);
          startOffset += 1;
          seconds = data.getUint8(startOffset);
          startOffset += 1;
        }

        if (numOfBytes >= 12) {
          microSecond = data.getUint32(startOffset, Endian.little);
          startOffset += 4;
        }

        hours += days * 24;
        final timeResult = StringBuffer();
        if (isNegative) {
          timeResult.write("-");
        }
        timeResult.write('${hours.toString().padLeft(2, '0')}:');
        timeResult.write('${minutes.toString().padLeft(2, '0')}:');
        timeResult.write(seconds.toString().padLeft(2, '0'));

        if (numOfBytes >= 12) {
          timeResult.write('.${microSecond.toString()}');
        }

        final consumed = startOffset - initialOffset;
        return Tuple2(timeResult.toString(), consumed);
      }

    case mysqlColumnTypeString:
    case mysqlColumnTypeVarString:
    case mysqlColumnTypeVarChar:
    case mysqlColumnTypeEnum:
    case mysqlColumnTypeSet:
      {
        final result = buffer.getUtf8LengthEncodedString(startOffset);
        return Tuple2(result.item1, result.item2);
      }

    case mysqlColumnTypeDecimal:
    case mysqlColumnTypeNewDecimal:
      {
        final lengthEncoded = buffer.getLengthEncodedBytes(startOffset);
        final strValue = String.fromCharCodes(lengthEncoded.item1);
        return Tuple2(strValue, lengthEncoded.item2);
      }

    case mysqlColumnTypeLongBlob:
    case mysqlColumnTypeMediumBlob:
    case mysqlColumnTypeBlob:
    case mysqlColumnTypeTinyBlob:
    case mysqlColumnTypeGeometry:
    case mysqlColumnTypeBit:
      {
        final lengthEncoded = buffer.getLengthEncodedBytes(startOffset);
        return Tuple2(lengthEncoded.item1, lengthEncoded.item2);
      }

    case mysqlColumnTypeYear:
      {
        final yearValue = data.getUint16(startOffset, Endian.little);
        return Tuple2(yearValue.toString(), 2);
      }
    case mysqlColumnTypeJson:
      {
        final lengthEncoded = buffer.getLengthEncodedBytes(startOffset);
        final jsonBytes = lengthEncoded.item1;
        final bytesConsumed = lengthEncoded.item2;

        try {
          final jsonString = utf8.decode(jsonBytes);
          final jsonObject = jsonDecode(jsonString);
          return Tuple2(jsonObject, bytesConsumed);
        } catch (e) {
          return Tuple2(utf8.decode(jsonBytes), bytesConsumed);
        }
      }
  }

  throw MySQLProtocolException(
    "Can not parse binary column data: column type $columnType is not implemented",
  );
}
