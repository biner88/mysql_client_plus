import 'dart:typed_data';
import 'package:mysql_client_plus/mysql_protocol.dart';
import 'package:mysql_client_plus/exception.dart';

class MySQLBinaryResultSetRowPacket extends MySQLPacketPayload {
  List<dynamic> values;

  MySQLBinaryResultSetRowPacket({
    required this.values,
  });

  factory MySQLBinaryResultSetRowPacket.decode(
    Uint8List buffer,
    List<MySQLColumnDefinitionPacket> colDefs,
  ) {
    final byteData = ByteData.sublistView(buffer);
    int offset = 0;

    // packet header (always should by 0x00)
    final type = byteData.getUint8(offset);
    offset += 1;

    if (type != 0) {
      throw MySQLProtocolException(
        "Can not decode MySQLBinaryResultSetRowPacket: packet type is not 0x00",
      );
    }

    List<dynamic> values = [];

    // parse null bitmap
    int nullBitmapSize = ((colDefs.length + 9) / 8).floor();

    final nullBitmap = Uint8List.sublistView(
      buffer,
      offset,
      offset + nullBitmapSize,
    );

    offset += nullBitmapSize;
    // parse binary data
    for (int x = 0; x < colDefs.length; x++) {
      // check null bitmap first
      final bitmapByteIndex = ((x + 2) / 8).floor();
      final bitmapBitIndex = (x + 2) % 8;

      final byteToCheck = nullBitmap[bitmapByteIndex];
      final isNull = (byteToCheck & (1 << bitmapBitIndex)) != 0;

      if (isNull) {
        values.add(null);
      } else {
        final parseResult = parseBinaryColumnData(
          colDefs[x].type.intVal,
          byteData,
          buffer,
          offset,
          colDefs[x].flags,
        );
        offset += parseResult.item2;
        values.add(parseResult.item1);
      }
    }

    return MySQLBinaryResultSetRowPacket(
      values: values,
    );
  }

  @override
  Uint8List encode() {
    throw UnimplementedError(
        "Encode not implemented for MySQLBinaryResultSetRowPacket");
  }
}
