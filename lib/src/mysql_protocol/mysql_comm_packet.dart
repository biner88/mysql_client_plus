//mysql_comm_packet.dart
import 'dart:convert';
import 'dart:typed_data';
import 'package:buffer/buffer.dart' show ByteDataWriter;
import 'package:mysql_client_plus/exception.dart';
import 'package:mysql_client_plus/mysql_protocol.dart';
import 'package:mysql_client_plus/mysql_protocol_extension.dart';

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
          // Se for nulo, o tipo é mysqlColumnTypeNull = 0x06
          buffer.writeUint8(mysqlColumnTypeNull);
          buffer.writeUint8(0); // Flag "unsigned" ou outro, geralmente 0
        } else {
          buffer.writeUint8(paramType.intVal);
          // Por exemplo, se quiser indicar "unsigned", poderia setar algo. Aqui, 0 = sem flag.
          buffer.writeUint8(0);
        }
      }

      // Escreve os valores dos parâmetros não-nulos
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

  void _writeParamValue(
    ByteDataWriter buffer,
    dynamic param,
    MySQLColumnType type,
  ) {
    switch (type.intVal) {
      case mysqlColumnTypeTiny: // 1 byte
        // Se o parâmetro for booleano, converte para 1 ou 0. Caso contrário, assume int 1 byte.
        if (param is bool) {
          buffer.writeUint8(param ? 1 : 0);
        } else {
          // Se param for int, convertendo para 8 bits (pode estourar se for >127).
          buffer.writeInt8(param);
        }
        break;

      case mysqlColumnTypeShort: // 2 bytes (int16)
        buffer.writeInt16(param, Endian.little);
        break;

      case mysqlColumnTypeLong: // 4 bytes (int32)
      case mysqlColumnTypeInt24: // no MySQL, 24 bits, mas normalmente tratamos c/ 32 bits
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
          // Se o parâmetro for Uint8List, manda-o como binário; caso contrário, converte para string UTF-8
          final encodedData = (param is Uint8List) ? param : utf8.encode(param.toString());

          // Primeiro escreve o tamanho (length-encoded)
          buffer.writeVariableEncInt(encodedData.length);
          // Depois escreve os bytes
          buffer.write(encodedData);
        }
        break;

      default:
        throw MySQLProtocolException(
          "Unsupported parameter type: ${type.intVal}",
        );
    }
  }

  /// Escreve um valor do tipo DateTime [dateTime] no [buffer] de acordo com o protocolo MySQL.
  ///
  /// Dependendo dos valores de ano, mês, dia, hora, minuto, segundo e microssegundos,
  /// o método escolhe um formato de 4, 7 ou 11 bytes.
  void _writeDateTime(ByteDataWriter buffer, DateTime dateTime) {
    final year = dateTime.year;
    final month = dateTime.month;
    final day = dateTime.day;
    final hour = dateTime.hour;
    final minute = dateTime.minute;
    final second = dateTime.second;
    final microsecond = dateTime.microsecond;

    // Caso todos os valores sejam zero, escreve 0 (sem dados de data/hora).
    if (year == 0 && month == 0 && day == 0 && hour == 0 && minute == 0 && second == 0 && microsecond == 0) {
      buffer.writeUint8(0);
      return;
    }

    if (microsecond > 0) {
      // 11 bytes: 1 de comprimento, 2 para ano, 1 p/ mês, 1 p/ dia,
      // 1 p/ hora, 1 p/ min, 1 p/ seg, 4 p/ microsegundos
      buffer.writeUint8(11);
      buffer.writeUint16(year, Endian.little);
      buffer.writeUint8(month);
      buffer.writeUint8(day);
      buffer.writeUint8(hour);
      buffer.writeUint8(minute);
      buffer.writeUint8(second);
      buffer.writeUint32(microsecond, Endian.little);
    } else if (hour > 0 || minute > 0 || second > 0) {
      // 7 bytes: 1 de comprimento, 2 p/ ano, 1 p/ mês, 1 p/ dia,
      // 1 p/ hora, 1 p/ min, 1 p/ seg
      buffer.writeUint8(7);
      buffer.writeUint16(year, Endian.little);
      buffer.writeUint8(month);
      buffer.writeUint8(day);
      buffer.writeUint8(hour);
      buffer.writeUint8(minute);
      buffer.writeUint8(second);
    } else {
      // 4 bytes: 1 de comprimento, 2 p/ ano, 1 p/ mês, 1 p/ dia
      buffer.writeUint8(4);
      buffer.writeUint16(year, Endian.little);
      buffer.writeUint8(month);
      buffer.writeUint8(day);
    }
  }

  /// Escreve um valor do tipo Time (representado como DateTime) no [buffer]
  /// de acordo com o protocolo MySQL.
  ///
  /// O protocolo binário do MySQL para TIME armazena:
  /// - 1 byte de "tamanho" (pode ser 0, 8 ou 12).
  /// - 1 byte de sinal (0=positivo, 1=negativo).
  /// - 4 bytes p/ "dias".
  /// - 1 hora, 1 min, 1 seg [=3 bytes].
  /// - Opcionalmente 4 bytes de microssegundos, se houver.
  ///
  /// Aqui, interpretamos [time] como um DateTime cujo dia/hora/min/seg representam
  /// apenas a parte de tempo (ex.: 00:00 até 23:59:59).
  void _writeTime(ByteDataWriter buffer, DateTime time) {
    final hour = time.hour;
    final minute = time.minute;
    final second = time.second;
    final microsecond = time.microsecond;

    // Se tudo zero, escreve 0 (tempo = 00:00:00).
    if (hour == 0 && minute == 0 && second == 0 && microsecond == 0) {
      buffer.writeUint8(0);
      return;
    }

    if (microsecond > 0) {
      // 12 bytes: 1 (len) + 1 (sinal) + 4 (dias=0) + 1 (hora) + 1 (min) + 1 (seg) + 4 (microseg)
      buffer.writeUint8(12);
      buffer.writeUint8(0); // sinal = 0 (positivo)
      buffer.writeUint32(0, Endian.little); // dias = 0
      buffer.writeUint8(hour);
      buffer.writeUint8(minute);
      buffer.writeUint8(second);
      buffer.writeUint32(microsecond, Endian.little);
    } else {
      // 8 bytes: 1 (len) + 1 (sinal) + 4 (dias=0) + 1 (hora) + 1 (min) + 1 (seg)
      buffer.writeUint8(8);
      buffer.writeUint8(0); // sinal = 0 (positivo)
      buffer.writeUint32(0, Endian.little); // dias = 0
      buffer.writeUint8(hour);
      buffer.writeUint8(minute);
      buffer.writeUint8(second);
    }
  }
}

/// Representa o comando COM_QUIT no protocolo MySQL.
///
/// Esse comando é utilizado para fechar a conexão com o servidor.
/// O pacote consiste apenas de um byte de comando (valor 1).
class MySQLPacketCommQuit extends MySQLPacketPayload {
  @override
  Uint8List encode() {
    final buffer = ByteDataWriter(endian: Endian.little);
    // Escreve o comando QUIT (1)
    buffer.writeUint8(1);
    return buffer.toBytes();
  }
}

/// Representa o comando COM_STMT_CLOSE no protocolo MySQL.
///
/// Esse comando é utilizado para fechar um prepared statement e liberar
/// os recursos associados no servidor. O pacote contém:
/// - Um byte de comando (valor 0x19).
/// - O ID do statement (stmtID) (4 bytes, little-endian).
class MySQLPacketCommStmtClose extends MySQLPacketPayload {
  /// ID do statement a ser fechado.
  final int stmtID;

  /// Construtor da classe.
  MySQLPacketCommStmtClose({
    required this.stmtID,
  });

  @override
  Uint8List encode() {
    final buffer = ByteDataWriter(endian: Endian.little);
    // Escreve o comando COM_STMT_CLOSE (0x19)
    buffer.writeUint8(0x19);
    // Escreve o statement ID (4 bytes, little-endian)
    buffer.writeUint32(stmtID, Endian.little);
    return buffer.toBytes();
  }
}
