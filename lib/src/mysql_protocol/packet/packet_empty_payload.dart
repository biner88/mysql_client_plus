import 'dart:typed_data';
import 'package:mysql_client_plus/mysql_protocol.dart';

class MySQLPacketEmptyPayload extends MySQLPacketPayload {
  @override
  Uint8List encode() {
    throw UnimplementedError(
        "Encode not implementado for MySQLPacketEmptyPayload");
  }
}
