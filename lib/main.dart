import 'dart:io';
import 'package:neat_periodic_task/neat_periodic_task.dart';
import 'package:firedart/firedart.dart';
import 'package:nag_me_lib/nag_me.dart';
import 'config.dart';
import 'hive_store.dart';

Future<void> main() async {
  // Login to fetch reminders etc from firebase:
  // Uri.resolve, cos File/Directory not smart enough?
  // print(Platform.script.resolve('firebase_config.yaml'));
  final fbConfig = FirebaseConfig(
      Platform.script.resolve('firebase_config.yaml').toFilePath());
  var fbAuth =
      FirebaseAuth.initialize(fbConfig.apiKey, await HiveStore.create());
  await FirebaseAuth.instance
      .signIn(fbConfig.loginEmail, fbConfig.loginPassword);
  fbAuth.signInState.listen((state) => print("Signed ${state ? "in" : "out"}"));
  // var admin = await FirebaseAuth.instance.getUser();

  // Cache of services (filled in [getChangedNotifiers]
  var _services = <String, Object>{};

  var firestore = Firestore(fbConfig.projectId, auth: fbAuth);
  // All users
  var users = await firestore.collection('users').get();
  // Fetch initial set of notifiers, we need to start a service per type:
  Map<String, Map<String, Notifier>> notifiers =
      await getChangedNotifiers(users, {}, _services);

  // Schedulers cache: [user.id][reminder.id][notifier.id] (firebase default ids)
  var schedulers =
      <String, Map<String, Map<String, NeatPeriodicTaskScheduler>>>{
    'firebase-poller': {
      'top': {'top': null}
    }
  };

  // Start services if applicable:
  startServices(_services);

  //final Map<String, Map<String, Map<String, NeatPeriodicTaskScheduler>>>> schedulers = {};
  schedulers['firebase-poller']['top']['top'] = NeatPeriodicTaskScheduler(
    interval: Duration(seconds: 60),
    name: 'firebase-poller',
    timeout: Duration(seconds: 5),
    task: () async {
      print('Polling....');
      // Current state of the notifiers, in case they changed
      // We'll stop/redo the changed ones
      // Map of user_id.notifier_id.NotifierObject
      notifiers = await getChangedNotifiers(users, notifiers, _services);
      print('fetched notifiers');
      print(notifiers.keys);
      // one schedule per reminder & notifier!?
      await Future.forEach(users, (user) async {
        var reminder_data = await user.reference.collection('reminders').get();
        reminder_data.forEach((doc) {
          Reminder reminder = Reminder.fromFirebase(doc, user.id);
          notifiers[user.id].keys.forEach((not_id) {
            // this is a new or modifield notifier, therefore stop the old one and
            // create another after (if its running)
            if (notifiers[user.id][not_id].has_changed &&
                schedulers.containsKey(user.id) &&
                schedulers[user.id].containsKey(doc.id) &&
                schedulers[user.id][doc.id].containsKey(not_id)) {
              schedulers[user.id][doc.id][not_id].stop();
              schedulers[user.id][doc.id].remove(not_id);
            }
            // Only create schedules that're supposed to start approximately now
            schedulers[user.id] ??= {};
            schedulers[user.id][doc.id] ??= {};

            var now = DateTime.now().toUtc();
            print(now.add(Duration(minutes: 5)).isAfter(reminder.next_time));
            print(now
                .subtract(Duration(minutes: 5))
                .isBefore(reminder.next_time));
            print(schedulers[user.id][doc.id].containsKey(not_id));
            if (now.add(Duration(minutes: 5)).isAfter(reminder.next_time) &&
                now
                    .subtract(Duration(minutes: 5))
                    .isBefore(reminder.next_time) &&
                !schedulers[user.id][doc.id].containsKey(not_id)) {
              print('Creating schedule for ${reminder.reminder_text}');
              schedulers[user.id][doc.id][not_id] = NeatPeriodicTaskScheduler(
                interval: Duration(minutes: 30),
                name: '${user.id}-${doc.id}-${not_id}',
                timeout: Duration(minutes: 5),
                task: () async {
                  print('Poking Telegram');
//                  await notifiers[user.id][not_id].settings.getUpdate();
                  await notifiers[user.id][not_id].settings.notifyUser();
                },
                minCycle: Duration(minutes: 3),
              );
              schedulers[user.id][doc.id][not_id].start();
            }
          });
        });
      });
    },
    minCycle: Duration(seconds: 10),
  );
  schedulers['firebase-poller']['top']['top'].start();
}

/// Fetches each user's notifier settings from FireBase
///
/// Each user may setup one or more notifiers, one of each type
/// We should only have one service object per notifier type, so we copy it across
/// returns a Map of user_id:notifier_id:Notifier object
Future<Map<String, Map<String, Notifier>>> getChangedNotifiers(
    users, Map notifiers, Map services) async {
  Map<String, Map<String, Notifier>> result = {};

  await Future.forEach(users, (user) async {
    var notification_data = await user.reference.collection('notifiers').get();
    await Future.forEach(notification_data, (notification) async {
      // Add service object if available

      Notifier checkNotifier = Notifier.fromFirebase(
          notification,
          user.id,
          services.containsKey(notification['engine'])
              ? services[notification['engine']]
              : null);
      // We need all of them, but note which were changed since we last looked
      if (notifiers.containsKey(user.id) &&
          notifiers[user.id].containsKey(notification.id)) {
        if (notifiers[user.id][notification.id]
            .last_modified
            .isBefore(checkNotifier.last_modified)) {
          checkNotifier.has_changed = true;
        }
        // Collect service object for this type of notifier
        services[notification['engine']] ??= notifiers[user.id][notification.id].settings.service;
      } else {
        services[notification['engine']] ??= checkNotifier.settings.service;
      }
      result[user.id] ??= {};
      result[user.id][notification.id] = checkNotifier;
    });
  });
  print('return: ${result.keys}');
  return result;
}

void startServices(Map services) {
  for (var service in services.values) {
    service.start();
  }
}
