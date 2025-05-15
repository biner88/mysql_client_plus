import 'dart:convert';
import 'dart:typed_data';
import 'package:buffer/buffer.dart';
import 'package:mysql_client_plus/mysql_protocol.dart';
import 'package:mysql_client_plus/mysql_protocol_extension.dart';

const _supportedCapabitilies = mysqlCapFlagClientProtocol41 |
    mysqlCapFlagClientSecureConnection |
    mysqlCapFlagClientPluginAuth |
    mysqlCapFlagClientPluginAuthLenEncClientData |
    mysqlCapFlagClientMultiStatements |
    mysqlCapFlagClientMultiResults;

class MySQLPacketHandshakeResponse41 extends MySQLPacketPayload {
  int capabilityFlags;
  int maxPacketSize;
  int characterSet;
  Uint8List authResponse;
  String authPluginName;
  String username;
  String? database;

  MySQLPacketHandshakeResponse41({
    required this.capabilityFlags,
    required this.maxPacketSize,
    required this.characterSet,
    required this.authResponse,
    required this.authPluginName,
    required this.username,
    this.database,
  });

  /// MYSQL_NATIVE_PASSWORD
  factory MySQLPacketHandshakeResponse41.createWithNativePassword({
    required String username,
    required String password,
    required MySQLPacketInitialHandshake initialHandshakePayload,
  }) {
    final challenge = initialHandshakePayload.authPluginDataPart1 +
        initialHandshakePayload.authPluginDataPart2!.sublist(0, 12);

    assert(challenge.length == 20);

    final passwordBytes = utf8.encode(password);

    final authData = xor(
      sha1(passwordBytes),
      sha1(challenge + sha1(sha1(passwordBytes))),
    );

    return MySQLPacketHandshakeResponse41(
      capabilityFlags: _supportedCapabitilies,
      maxPacketSize: 50 * 1024 * 1024,
      authPluginName: initialHandshakePayload.authPluginName!,
      characterSet: initialHandshakePayload.charset,
      authResponse: authData,
      username: username,
    );
  }

  /// CACHING_SHA2_PASSWORD
  factory MySQLPacketHandshakeResponse41.createWithCachingSha2Password({
    required String username,
    required String password,
    required MySQLPacketInitialHandshake initialHandshakePayload,
  }) {
    final challenge = initialHandshakePayload.authPluginDataPart1 +
        initialHandshakePayload.authPluginDataPart2!.sublist(0, 12);

    assert(challenge.length == 20);

    final passwordBytes = utf8.encode(password);

    final authData = xor(
      sha256(passwordBytes),
      sha256(sha256(sha256(passwordBytes)) + challenge),
    );

    return MySQLPacketHandshakeResponse41(
      capabilityFlags: _supportedCapabitilies,
      maxPacketSize: 50 * 1024 * 1024,
      authPluginName: initialHandshakePayload.authPluginName!,
      characterSet: initialHandshakePayload.charset,
      authResponse: authData,
      username: username,
    );
  }

  /// SHA256_PASSWORD
  factory MySQLPacketHandshakeResponse41.createWithSha256Password({
    required String username,
    required String password,
    required MySQLPacketInitialHandshake initialHandshakePayload,
    required bool secure,
  }) {
    final challenge = initialHandshakePayload.authPluginDataPart1 +
        initialHandshakePayload.authPluginDataPart2!.sublist(0, 12);

    assert(challenge.length == 20);

    Uint8List authData;

    if (secure) {
      authData = Uint8List.fromList(utf8.encode('$password\u0000'));
    } else {
      authData = Uint8List(0);
    }

    return MySQLPacketHandshakeResponse41(
      capabilityFlags: _supportedCapabitilies,
      maxPacketSize: 50 * 1024 * 1024,
      authPluginName: 'sha256_password',
      characterSet: initialHandshakePayload.charset,
      authResponse: authData,
      username: username,
    );
  }
  @override
  Uint8List encode() {
    final buffer = ByteDataWriter(endian: Endian.little);

    if (database != null) {
      capabilityFlags = capabilityFlags | mysqlCapFlagClientConnectWithDB;
    }

    buffer.writeUint32(capabilityFlags);
    buffer.writeUint32(maxPacketSize);
    buffer.writeUint8(characterSet);
    buffer.write(List.filled(23, 0));
    buffer.write(utf8.encode(username));
    buffer.writeUint8(0);

    if (capabilityFlags & mysqlCapFlagClientSecureConnection != 0) {
      buffer.writeVariableEncInt(authResponse.lengthInBytes);
      buffer.write(authResponse);
    }

    if (database != null &&
        capabilityFlags & mysqlCapFlagClientConnectWithDB != 0) {
      buffer.write(utf8.encode(database!));
      buffer.writeUint8(0);
    }

    if (capabilityFlags & mysqlCapFlagClientPluginAuth != 0) {
      buffer.write(utf8.encode(authPluginName));
      buffer.writeUint8(0);
    }

    return buffer.toBytes();
  }
}
