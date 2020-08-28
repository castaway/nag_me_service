

import './scheduler.dart';

Future<void> main() async {
  var scheduling = Scheduler();
  await scheduling.setup();
  await scheduling.updateFromStorage();
  await scheduling.start();
}