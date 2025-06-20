import 'dart:convert';
import 'dart:typed_data';
import 'package:mysql_client_plus/mysql_protocol.dart';
import 'package:mysql_client_plus/mysql_protocol_extension.dart';

class MySQLResultSetRowPacket extends MySQLPacketPayload {
  List<dynamic> values;

  MySQLResultSetRowPacket({
    required this.values,
  });

  factory MySQLResultSetRowPacket.decode(
    Uint8List buffer,
    List<MySQLColumnDefinitionPacket> columns,
  ) {
    final byteData = ByteData.sublistView(buffer);
    int offset = 0;
    final values = <dynamic>[];

    for (int x = 0; x < columns.length; x++) {
      final colDef = columns[x];
      final nextByte = byteData.getUint8(offset);

      // 0xFB = NULL
      if (nextByte == 0xfb) {
        values.add(null);
        offset++;
      } else {
        if (_isBinaryType(colDef.type)) {
          final isBinary = (colDef.flags & 0x80) != 0;
          if (isBinary) {
            final lengthEncoded = buffer.getLengthEncodedBytes(offset);
            offset += lengthEncoded.item2;
            values.add(lengthEncoded.item1);
          } else {
            final lengthEncoded = buffer.getUtf8LengthEncodedString(offset);
            offset += lengthEncoded.item2;
            values.add(lengthEncoded.item1);
          }
        } else if (colDef.type.intVal == MySQLColumnType.bitType.intVal) {
          final lengthEncoded = buffer.getLengthEncodedBytes(offset);
          offset += lengthEncoded.item2;
          values.add(lengthEncoded.item1);
        } else if (colDef.type.intVal == MySQLColumnType.jsonType.intVal) {
          final lengthEncoded = buffer.getUtf8LengthEncodedString(offset);
          offset += lengthEncoded.item2;
          try {
            final jsonObject = jsonDecode(lengthEncoded.item1);
            values.add(jsonObject);
          } catch (e) {
            values.add(lengthEncoded.item1);
          }
        } else {
          final lengthEncoded = buffer.getUtf8LengthEncodedString(offset);
          offset += lengthEncoded.item2;
          values.add(lengthEncoded.item1);
        }
      }
    }

    return MySQLResultSetRowPacket(values: values);
  }

  @override
  Uint8List encode() {
    throw UnimplementedError();
  }

  static bool _isBinaryType(MySQLColumnType colType) {
    return colType.intVal == MySQLColumnType.tinyBlobType.intVal ||
        colType.intVal == MySQLColumnType.mediumBlobType.intVal ||
        colType.intVal == MySQLColumnType.longBlobType.intVal ||
        colType.intVal == MySQLColumnType.blobType.intVal ||
        colType.intVal == MySQLColumnType.geometryType.intVal;
  }
}
