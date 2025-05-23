import 'dart:typed_data';
import 'package:mysql_client_plus/mysql_protocol.dart';

class MySQLPacketBinaryResultSet extends MySQLPacketPayload {
  BigInt columnCount;
  List<MySQLColumnDefinitionPacket> columns;
  List<MySQLBinaryResultSetRowPacket> rows;

  MySQLPacketBinaryResultSet({
    required this.columnCount,
    required this.columns,
    required this.rows,
  });

  @override
  Uint8List encode() {
    throw UnimplementedError(
        "Encode not implementado for MySQLPacketBinaryResultSet");
  }
}
