import 'dart:io';
import 'package:neat_periodic_task/neat_periodic_task.dart';
import 'package:firedart/firedart.dart';
import 'package:logging/logging.dart';
import 'package:nag_me_lib/nag_me.dart';
import 'package:nag_me_lib/nag_me_services.dart';
import 'config.dart';
import 'hive_store.dart';

class Scheduler {
  FirebaseConfig _fbConfig;
  FirebaseAuth _fbAuth;
  Firestore _firestore;
  Page<Document> _users;
  final _schedulers;
  var _userData = <String, Map>{};
  var _serviceData = {};
  var _services = <String, NotifierService>{};
  Map<String, Map<String, Notifier>> _notifiers;

  Scheduler()
      : _schedulers =
            <String, Map<String, Map<String, NeatPeriodicTaskScheduler>>>{
          'firebase-poller': {
            'top': {'top': null}
          }
        };

  // do once actions
  Future<void> setup() async {
    _fbConfig = FirebaseConfig(
        Platform.script.resolve('firebase_config.yaml').toFilePath());

    _fbAuth =
        FirebaseAuth.initialize(_fbConfig.apiKey, await HiveStore.create());

    await FirebaseAuth.instance
        .signIn(_fbConfig.loginEmail, _fbConfig.loginPassword);
    _fbAuth.signInState
        .listen((state) => print("Signed ${state ? "in" : "out"}"));
    _firestore = Firestore(_fbConfig.projectId, auth: _fbAuth);
  }

  Future updateFromStorage() async {
    // All users
    _users = await _firestore.collection('users').get();
    // user saved data:
    _userData = {};
    await Future.forEach(_users, (user) {
      _userData[user.id] = user.map;
    });

    // service saved data:
    final _savedServiceData = await _firestore.collection('services').get();
    _serviceData = {};
    await Future.forEach(_savedServiceData, (service) {
      _serviceData[service.id] = service.map;
    });

    // Cache of services
    // TODO: make stop/restart, but only if changed!
    loadServices();

    // Fetch initial set of notifiers, we need to start a service per type:
    _notifiers = await getChangedNotifiers(_users, _notifiers ?? {}, _services);
  }

  Future<void> start() async {
    // Schedulers cache: [user.id][reminder.id][notifier.id] (firebase default ids)
    // Start services if applicable:

    // TODO: Stop+start if we loaded new services!
    // Should do this in/from updateFromStorage?
    startServices(_services);

    // Configure logging so that we see scheduler output/debugging:
    Logger.root.level = Level.ALL; // defaults to Level.INFO
    Logger.root.onRecord.listen((record) {
      print('${record.level.name}: ${record.time}: ${record.message}');
      if(record.error != null) {
        print('Error: ${record.error}, StackTrace: ${record.stackTrace
            .toString()}');
      }
    });

    //final Map<String, Map<String, Map<String, NeatPeriodicTaskScheduler>>>> schedulers = {};
    _schedulers['firebase-poller']['top']['top'] = NeatPeriodicTaskScheduler(
      interval: Duration(seconds: 60),
      name: 'firebase-poller',
      timeout: Duration(seconds: 5),
      task: () async {
        print('Polling....');
        // Current state of the notifiers, in case they changed
        // We'll stop/redo the changed ones
        // Map of user_id.notifier_id.NotifierObject
        await updateFromStorage();
        print(_notifiers.keys);
        var reminders = <Reminder>[];
        // one schedule per reminder & notifier!?
        await Future.forEach(_users, (user) async {
          var reminder_data =
              await user.reference.collection('reminders').get();
          reminder_data.forEach((doc) {
            var reminder = Reminder.fromFirebase(doc, doc.id, user.id);
            reminders.add(reminder);
            _notifiers[user.id].keys.forEach((not_id) {
              // this is a new or modifield notifier, therefore stop the old one and
              // create another after (if its running)
              if (_notifiers[user.id][not_id].has_changed &&
                  _schedulers.containsKey(user.id) &&
                  _schedulers[user.id].containsKey(doc.id) &&
                  _schedulers[user.id][doc.id].containsKey(not_id)) {
                _schedulers[user.id][doc.id][not_id].stop();
                _schedulers[user.id][doc.id].remove(not_id);
              }

              _schedulers[user.id] ??=
                  <String, Map<String, NeatPeriodicTaskScheduler>>{};
              _schedulers[user.id]
                  [doc.id] ??= <String, NeatPeriodicTaskScheduler>{};

              // Only create schedules that're supposed to start approximately now
              var now = DateTime.now().toUtc();
              print('now: ${now.toIso8601String()}');
              print('next_time: ${reminder.next_time.toIso8601String()}');
              if (now.add(Duration(minutes: 5)).isAfter(reminder.next_time) &&
                  now
                      .subtract(Duration(minutes: 5))
                      .isBefore(reminder.next_time) &&
                  !_schedulers[user.id][doc.id].containsKey(not_id)) {
                print('Creating schedule for ${reminder.reminder_text}');
                // TODO: This should remove the schedule if for some reason
                // it couldnt be started properly (to retry again next time)
                _schedulers[user.id][doc.id][not_id] =
                    NeatPeriodicTaskScheduler(
                  interval: Duration(minutes: 30),
                  name: '${user.id}-${doc.id}-${not_id}',
                  timeout: Duration(minutes: 5),
                  task: () async {
                    print('Poking Notifiers');
                    final result = await _notifiers[user.id][not_id]
                        .settings
                        .notifyUser(reminder);
                    print(
                        'Notified via ${_notifiers[user.id][not_id].engine.toString()}: $result');
                  },
                  minCycle: Duration(minutes: 3),
                );
                _schedulers[user.id][doc.id][not_id].start();
              }
            }); // notifiers.forEach
          }); // remider_data.forEach
        }); // Future.forEach

        // stop/remove any that have been responded to
        checkSchedulers(reminders);
        // This may have updated reminders
        await updateReminders(_firestore, reminders);

        // check if anyone asked us any questions
        respondToQueries(reminders);
      },
      minCycle: Duration(seconds: 10),
    );
    _schedulers['firebase-poller']['top']['top'].start();
    // ctrl-c?
    await ProcessSignal.sigint.watch().first;
    // shut down main, backup services
    await _schedulers['firebase-poller']['top']['top'].stop();
    await saveServices(_firestore, _services);
  }

  void loadServices() {
    TelegramService.getInstance().then((teleService) {
      teleService.fromFirebase(_serviceData['Telegram']);
      teleService.fromUser(_userData);
      _services['Engine.Telegram'] ??= teleService;
    });

    MobileService.getInstance().then((mobileService) {
       mobileService.fromFirebase(_serviceData['Mobile']);
       mobileService.fromUser(_userData);
       _services['Engine.Mobile'] ??= mobileService;
    });
  }

  /// Fetches each user's notifier settings from FireBase
  ///
  /// Each user may setup one or more notifiers, one of each type
  /// We should only have one service object per notifier type, so we copy it across
  /// returns a Map of user_id:notifier_id:Notifier object
  Future<Map<String, Map<String, Notifier>>> getChangedNotifiers(
      users, Map notifiers, Map services) async {
    var result = <String, Map<String, Notifier>>{};

    await Future.forEach(users, (user) async {
      var notification_data =
          await user.reference.collection('notifiers').get();
      await Future.forEach(notification_data, (notification) async {
        // Add service object if available

        var checkNotifier = Notifier.fromFirebase(
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
        }
        if(services.containsKey(notification['engine'])) {
          services[notification['engine']]
            .userKeys[checkNotifier.settings.username] ??= user.id;
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
      // service.stop();
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

  void checkSchedulers(List reminders) {
    // Check if any tasks have been done (or claimed to be!)
    _services.forEach((name, service) {
      // finished Reminder ids:
      List<dynamic> done = service.getFinishedTasks();

      // need to find the matching schedulers
      // users whose reminders are done (keys of done)
      // we don't need to find which notifiers as we want to
      // stop/remove all of them for this reminder

      // Lots of shallow copies (Map.from) in this, else we can't remove() the item at the end
      var endingSchedules = [];
      var schedulersCopy = Map.from(_schedulers);
      for (var user in schedulersCopy.entries) {
        var docsCopy = Map.from(user.value);
        endingSchedules =
            docsCopy.entries.where((doc) => done.contains(doc.key)).toList();

        for (var doc in endingSchedules) {
          var doc_id = doc.key;
          var remIndex = reminders.indexWhere((rem) => rem.id == doc_id);
          // Need to update the reminder object in place
          reminders[remIndex].taskDone();
          var notsCopy = Map.from(doc.value);
          for (var notification in notsCopy.entries) {
            notification.value.stop();
            _schedulers[reminders[remIndex].owner_id][doc_id]
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

  // Poll services for any incoming queries
  void respondToQueries(List reminders) {
    // test:
    // _services['Engine.Telegram'].sendMessage('castaway', 'testid', 'blahblah');
    _services.forEach((name, service) {
      // finished Reminder ids:
      Map queries = service.incomingCommands;

      for (var user_id in queries.keys) {
        for (var command in queries[user_id]) {
          MapEntry who = service.userKeys.entries
              .firstWhere((entry) => entry.value == user_id, orElse: () {
            print('No matching users');
            return null;
          });

          // Using the id of the incoming message as the key to follow any responses
          if (command['text'] == '/reminders') {
            print('Responding...');
            reminders.sort((r1, r2) => r1.next_time.compareTo(r2.next_time));
            int counter = 1;
            String reminderStr =
                reminders.map((reminder) => '${counter++}: ${reminder.displayString()}').join('\n');
            print(reminderStr);
            // print('Send: $reminderStr to ${who.key}, id: ${command['id']}');
            Map reminderList = {
              'text': reminderStr,
              'create_buttons': List.generate(counter, (index) => '/edit ${index+1}'),
            };
            service.sendMessage(who.key, command['id'].toString(), reminderList);
          }
        }
      }

      service.clearCommands();
    }); // _services.forEach
  }
}
