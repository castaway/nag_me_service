import 'dart:io';
import 'package:neat_periodic_task/neat_periodic_task.dart';
import 'package:firedart/firedart.dart';
import 'package:nag_me_lib/nag_me.dart';
import 'package:nag_me_lib/nag_me_services.dart';
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

  var firestore = Firestore(fbConfig.projectId, auth: fbAuth);
  // All users
  var users = await firestore.collection('users').get();
  // user saved data:
  var userData = {};
  await Future.forEach(users, (user) {
    userData[user.id] = user.map;
  });

  // service saved data:
  var savedServiceData = await firestore.collection('services').get();
  var serviceData = {};
  await Future.forEach(savedServiceData, (service) {
    serviceData[service.id] = service.map;
  });
  // Cache of services (filled in [getChangedNotifiers]
  var _services = loadServices(serviceData, userData);

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
      print(notifiers.keys);
      List<Reminder> reminders = [];
      // one schedule per reminder & notifier!?
      await Future.forEach(users, (user) async {
        var reminder_data = await user.reference.collection('reminders').get();
        reminder_data.forEach((doc) {
          Reminder reminder = Reminder.fromFirebase(doc, doc.id, user.id);
          reminders.add(reminder);
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

            schedulers[user.id] ??= {};
            schedulers[user.id][doc.id] ??= {};

            // Only create schedules that're supposed to start approximately now
            var now = DateTime.now().toUtc();
            print('now: ${now.toIso8601String()}');
            print('next_time: ${reminder.next_time.toIso8601String()}');
            if (now.add(Duration(minutes: 5)).isAfter(reminder.next_time) &&
                now
                    .subtract(Duration(minutes: 5))
                    .isBefore(reminder.next_time) &&
                !schedulers[user.id][doc.id].containsKey(not_id)) {
              print('Creating schedule for ${reminder.reminder_text}');
              // TODO: This should remove the schedule if for some reason
              // it couldnt be started properly (to retry again next time)
              schedulers[user.id][doc.id][not_id] = NeatPeriodicTaskScheduler(
                interval: Duration(minutes: 30),
                name: '${user.id}-${doc.id}-${not_id}',
                timeout: Duration(minutes: 5),
                task: () async {
                  print('Poking Notifiers');
//                  await notifiers[user.id][not_id].settings.getUpdate();
                  final result = await notifiers[user.id][not_id]
                      .settings
                      .notifyUser(reminder);
                  print('Notified via Telegram: $result');
                },
                minCycle: Duration(minutes: 3),
              );
              schedulers[user.id][doc.id][not_id].start();
            }
          }); // notifiers.forEach
        }); // remider_data.forEach
      }); // Future.forEach

      // stop/remove any that have been responded to
      checkSchedulers(_services, schedulers, reminders);
      // This may have updated reminders
      await updateReminders(firestore, reminders);
    },
    minCycle: Duration(seconds: 10),
  );
  schedulers['firebase-poller']['top']['top'].start();
  // ctrl-c?
  await ProcessSignal.sigint.watch().first;
  // shut down main, backup services
  await schedulers['firebase-poller']['top']['top'].stop();
  await saveServices(firestore, _services);
}

// NotifierService?
Map<String, NotifierService> loadServices(Map serviceData, Map userData) {
  TelegramService teleService = TelegramService();
  MobileService mobileService = MobileService();
  teleService.fromFirebase(serviceData['Telegram']);
  teleService.fromUser(userData);
  mobileService.fromFirebase(serviceData['Mobile']);
  mobileService.fromUser(userData);


  return <String, NotifierService>{
    'Engine.Telegram': teleService,
    'Engine.Mobile': mobileService,
  };
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
        services[notification['engine']] ??=
            notifiers[user.id][notification.id].settings.service;
      } else {
        services[notification['engine']] ??= checkNotifier.settings.service;
      }
//      if(serviceData != null) {
//        services[notification['engine']].fromFirebase(
//            serviceData[checkNotifier.settings.name]);
//      }
      result[user.id] ??= {};
      result[user.id][notification.id] = checkNotifier;
    });
  });
  return result;
}

void startServices(Map services) {
  for (var service in services.values) {
    service.start();
  }
}

Future<void> saveServices(firestore, Map services) async {
  for (var service in services.values) {
    var saveData = service.forFirebase();
    await firestore
        .collection('services')
        .document(saveData['name'])
        .set(saveData['data']);
  }
}

void checkSchedulers(Map services, Map schedulers, List reminders) {
  // Check if any tasks have been done (or claimed to be!)
  services.forEach((name, service) {
    // finished Reminder ids:
    List<dynamic> done = service.getFinishedTasks();

    // need to find the matching schedulers
    // users whose reminders are done (keys of done)
    // we don't need to find which notifiers as we want to
    // stop/remove all of them for this reminder

    // Lots of shallow copies (Map.from) in this, else we can't remove() the item at the end
    var endingSchedules = [];
    var schedulersCopy = Map.from(schedulers);
    for (MapEntry user in schedulersCopy.entries) {
      var docsCopy = Map.from(user.value);
      endingSchedules =
          docsCopy.entries.where((doc) => done.contains(doc.key)).toList();

      for (MapEntry doc in endingSchedules) {
        var doc_id = doc.key;
        var remIndex = reminders.indexWhere((rem) => rem.id == doc_id);
        // Need to update the reminder object in place
        reminders[remIndex].taskDone();
        var notsCopy = Map.from(doc.value);
        for (MapEntry notification in notsCopy.entries) {
          notification.value.stop();
          schedulers[reminders[remIndex].owner_id][doc_id]
              .remove(notification.key);
        } // notifications
      } // docs
    } // users
  }); // _services.forEach
}

// Store the updated "next dates" back to Firestore.
// Should probably only do the changed ones
void updateReminders(Firestore firestore, List reminders) async {
  for (Reminder reminder in reminders) {
    var asMap = reminder.toMap();
    await firestore
        .collection('users')
        .document(asMap['owner_id'])
        .collection('reminders')
        .document(reminder.id)
        .set(asMap);
  }
}
