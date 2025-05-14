import 'dart:convert';
import 'dart:typed_data';
import 'package:buffer/buffer.dart';
import 'package:mysql_client_plus/mysql_protocol.dart';
import 'package:pointycastle/export.dart';
import 'package:pointycastle/pointycastle.dart';

class MySQLPacketAuthMoreData extends MySQLPacketPayload {
  final Uint8List pluginData;

  MySQLPacketAuthMoreData(this.pluginData);

  factory MySQLPacketAuthMoreData.decode(ByteDataReader reader) {
    final remaining = reader.remainingLength;
    return MySQLPacketAuthMoreData(reader.read(remaining));
  }

  ///  Parses a PEM-encoded RSA public key into a RSAPublicKey object.
  static RSAPublicKey _parsePublicKeyFromPem(String pem) {
    final lines = pem.split('\n').where((line) => !line.startsWith('---') && line.trim().isNotEmpty).toList();

    final base64Str = lines.join('');
    final derBytes = base64.decode(base64Str);
    final asn1Parser = ASN1Parser(derBytes);

    final topLevelSeq = asn1Parser.nextObject() as ASN1Sequence;
    final publicKeyBitString = topLevelSeq.elements![1] as ASN1BitString;

    final publicKeyAsn = ASN1Parser(publicKeyBitString.stringValues as Uint8List);
    final publicKeySeq = publicKeyAsn.nextObject() as ASN1Sequence;

    final modulus = publicKeySeq.elements![0] as ASN1Integer;
    final exponent = publicKeySeq.elements![1] as ASN1Integer;

    return RSAPublicKey(modulus.integer!, exponent.integer!);
  }

  /// Encrypts the password using RSA encryption with OAEP padding.
  static Uint8List encryptPasswordWithRSA({
    required String password,
    required String publicKeyPem,
  }) {
    final publicKey = _parsePublicKeyFromPem(publicKeyPem);

    final encryptor = OAEPEncoding(RSAEngine())..init(true, PublicKeyParameter<RSAPublicKey>(publicKey));

    final plaintext = Uint8List.fromList(utf8.encode('$password\u0000'));
    return encryptor.process(plaintext);
  }

  @override
  Uint8List encode() {
    final buffer = ByteDataWriter();
    buffer.writeUint8(0x01);
    buffer.write(pluginData);
    return buffer.toBytes();
  }
}
