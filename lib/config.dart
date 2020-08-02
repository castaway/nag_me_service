import 'dart:io';
import 'package:safe_config/safe_config.dart';

class FirebaseConfig extends Configuration {
  FirebaseConfig(String fileName) : super.fromFile(File(fileName));

  String apiKey;
  String loginEmail;
  String loginPassword;

  @optionalConfiguration
  String projectId = 'nag-me-84934';

}
