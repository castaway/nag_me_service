import 'dart:io';
import 'package:neat_periodic_task/neat_periodic_task.dart';
import 'package:firedart/firedart.dart';
import 'package:logging/logging.dart';
import 'package:email_validator/email_validator.dart';
import 'package:time_machine/time_machine.dart';
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
  Map<String, Map> _userData = <String, Map>{};
  var _serviceData = {};
  Map<String, NotifierService> _services = <String, NotifierService>{};
  Map<String, Map<String, Notifier>> _notifiers;
  var _reminders = <Reminder>[];

  Scheduler()
      : _schedulers =
            <String, Map<String, Map<String, NeatPeriodicTaskScheduler>>>{
          'firebase-poller': {
            'top': {'top': null}
          }
        };

  // do once actions
  Future<void> setup() async {
    await TimeMachine.initialize();
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
    // timezone db:
    final tzdb = await DateTimeZoneProviders.tzdb;

    // All users
    this._users = await _firestore.collection('users').get();
    // user saved data:
    // Map<String, dynamic>_userData = {};
    await Future.forEach(this._users, (Document user) async {
        _userData[user.id] = user.map;
        if (_userData[user.id].containsKey('timezone') &&
            tzdb.getZoneOrNull(_userData[user.id]['timezone'].toString()) !=
                null) {
          _userData[user.id]['zone'] =
          await tzdb[_userData[user.id]['timezone'].toString()];
        } else {
          _userData[user.id]['zone'] = await tzdb.getSystemDefault();
        }
    });

    // service saved data:
    final Page<Document>_savedServiceData = await _firestore.collection('services').get();
    _serviceData = {};
    await Future.forEach(_savedServiceData, (Document service) {
      _serviceData[service.id] = service.map;
    });

    // Cache of services
    // TODO: make stop/restart, but only if changed!
    loadServices();

    // Fetch initial set of notifiers, we need to start a service per type:
    _notifiers = await getChangedNotifiers(_users, _notifiers ?? {}, _services);

    // Load reminders
    _reminders = await getReminders(_users);
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
      if (record.error != null) {
        print(
            'Error: ${record.error}, StackTrace: ${record.stackTrace.toString()}');
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
        // one schedule per reminder & notifier!?
        await Future.forEach(_users, (Document user) async {
          _reminders.where((r) => r.owner_id == user.id).forEach((reminder) {
            // Skip if this reminder is "off"
            if (reminder.status == ReminderStatus.off) {
              return;
            }
            if (_notifiers.containsKey(user.id) && _notifiers[user.id] != null ) {
//              Map<String, Notifier> userNotifier = _notifiers[user.id];
              _notifiers[user.id].keys.forEach((not_id) {
                // this is a new or modified notifier, therefore stop the old one and
                // create another after (if its running)
                if (_notifiers[user.id][not_id].has_changed &&
                    _schedulers.containsKey(user.id) &&
                    _schedulers[user.id].containsKey(reminder.id) &&
                    _schedulers[user.id][reminder.id].containsKey(not_id)) {
                  _schedulers[user.id][reminder.id][not_id].stop();
                  _schedulers[user.id][reminder.id].remove(not_id);
                }

                if(
                     _schedulers.containsKey(user.id) &&
                     _schedulers[user.id].containsKey(reminder.id) &&
                     _schedulers[user.id][reminder.id].containsKey(not_id) &&
                //     _schedulers?[user.id]?[reminder.id]?[not_id] != null &&
                    reminder.aboutToOverrun()) {
                    logStatus({
                      'time': DateTime.now().
                      toIso8601String(),
                      'status': 'OverrunTask',
                      'reminder': reminder.id,
                  }, user.id);

                  _schedulers[user.id][reminder.id][not_id].stop();
                  _schedulers[user.id][reminder.id].remove(not_id);

                  reminder.taskDone();
                  _notifiers[user.id][not_id].settings.stopReminder(reminder);

                }

                _schedulers[user.id] ??=
                <String, Map<String, NeatPeriodicTaskScheduler>>{};
                _schedulers[user.id]
                [reminder.id] ??= <String, NeatPeriodicTaskScheduler>{};

                // Only create schedules that're supposed to start approximately now
                var now = Instant.now();
                // Start if:
                // Not already got a scheduler for this item AND
                // it should be running (status set but isnt because service crashed for eg) OR
                // current time is after the time it should have been started
                if (!_schedulers[user.id][reminder.id].containsKey(not_id) && (
                  reminder.status == ReminderStatus.running || (
                    now.add(Time(minutes: 1)).isAfter(reminder.next_time)
                    // && now.subtract(Duration(minutes: 5)).isBefore(reminder.next_time)
                    )
                )
                ) {
                  print('Creating schedule for ${reminder.reminder_text}');
                  // TODO: This should remove the schedule if for some reason
                  // it couldnt be started properly (to retry again next time)
                  _schedulers[user.id][reminder.id][not_id] =
                      NeatPeriodicTaskScheduler(
                        interval: Duration(minutes: 30),
                        name: '${user.id}-${reminder.id}-${not_id}',
                        timeout: Duration(minutes: 5),
                        task: () async {
                          print('Poking Notifiers');
                          logStatus({
                            'time': DateTime.now().
                            toIso8601String(),
                            'status': 'notifyUser',
                            'reminder': reminder.id,
                            'notification': not_id,
                          }, user.id);
                          final result = await _notifiers[user.id][not_id]
                              .settings
                              .notifyUser(reminder);
                          print(
                              'Notified via ${_notifiers[user.id][not_id].engine
                                  .toString()}: $result');
                        },
                        minCycle: Duration(minutes: 3),
                      );
                  logStatus({
                    'time': DateTime.now().
                    toIso8601String(),
                    'status': 'createScheduler',
                    'reminder': reminder.id,
                    'notification': not_id,
                  }, user.id);
                  _schedulers[user.id][reminder.id][not_id].start();
                  reminder.status = ReminderStatus.running;
                }
              }); // notifiers.forEach
            }
          }); // remider_data.forEach
        }); // Future.forEach

        // stop/remove any that have been responded to
        // checkSchedulers();
        // Save reminders where status changed:
//        await updateReminders();
        updateReminders();

        // check if anyone asked us any questions
        //respondToQueries();
      },
      minCycle: Duration(seconds: 10),
    );
    _schedulers['firebase-poller']['top']['top'].start();
    // ctrl-c?
    await ProcessSignal.sigint.watch().first;
    // shut down main, backup services
    await _schedulers['firebase-poller']['top']['top'].stop();
    await saveServices();
  }

  void loadServices() {
    Map<String, Function> callbacks = {
      'reminder_list': getReminderList,
      'finish_task': finishTask,
      'update_reminders': updateReminders,
      'check_user': checkUser,
      'create_user': createUser,
      'update_user': updateUser,
    };
    TelegramService.getInstance(callbacks).then((teleService) {
      teleService.fromFirebase(_serviceData['Telegram']);
      teleService.fromUser(_userData);
      _services['Engine.Telegram'] ??= teleService;
    });

    MobileService.getInstance(callbacks).then((mobileService) {
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

    await Future.forEach(users, (Document user) async {
      Page<Document> notification_data =
          await user.reference.collection('notifiers').get();
      await Future.forEach(notification_data, (Document notification) async {
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
        if (services.containsKey(notification['engine'])) {
          services[notification['engine']]
              .userKeys[checkNotifier.settings.username] ??= user.id;
        }
//      if(serviceData != null) {
//        services[notification['engine']].fromFirebase(
//            serviceData[checkNotifier.settings.name]);
//      }
        result[user.id] ??= <String, Notifier>{};
        result[user.id][notification.id] = checkNotifier;
      });
    });
    return result;
  }

  Future<List<Reminder>> getReminders(users) async {
    List<Reminder> reminders = [];
    await Future.forEach(users, (Document user) async {
      var reminder_data =
      await user.reference.collection('reminders').get();
      reminder_data.forEach((Document doc) {
        var reminder = Reminder.fromFirebase(doc, doc.id, user.id, _userData[user.id]['zone']);
        reminders.add(reminder);
      });
    });
    return reminders;
  }

  void startServices(Map services) {
    for (var service in services.values) {
      // service.stop();
      service.start();
    }
  }

  Future<void> saveServices() async {
    for (var service in _services.values) {
      var saveData = service.forFirebase();
      await _firestore
          .collection('services')
          .document(saveData['name'])
          .set(saveData['data']);
    }
  }

  // Callback: Stop running scheduler(s), update time on finished reminder
  Future<bool> finishTask(String user_id, String reminderId) async {
    // Lots of shallow copies (Map.from) in this, else we can't remove() the item at the end
    var endingSchedules = [];
    logStatus({
      'time': DateTime.now().
      toIso8601String(),
      'status': 'finishTask',
      'reminder': reminderId,
    }, user_id);

    var schedulersCopy = Map.from(_schedulers);
    for (var user in schedulersCopy.entries) {
      var docsCopy = Map.from(user.value);
      endingSchedules =
          docsCopy.entries.where((doc) => reminderId == doc.key).toList();

      for (var doc in endingSchedules) {
        var doc_id = doc.key;
        var remIndex = _reminders.indexWhere((rem) => rem.id == doc_id);
        // Need to update the reminder object in place
        _reminders[remIndex].taskDone();
        var notsCopy = Map.from(doc.value);
        for (var notification in notsCopy.entries) {
          notification.value.stop();
          _schedulers[_reminders[remIndex].owner_id][doc_id]
              .remove(notification.key);
        } // notifications
      } // docs
    } // users
    updateReminders();
    return true;
  }


// Store the updated "next dates" back to Firestore.
// Should probably only do the changed ones
  void updateReminders([List<Reminder> reminders]) async {
    if (reminders != null) {
      this._reminders = reminders;
    }
    for (Reminder reminder in _reminders) {
      var asMap = reminder.toMap();
      var owner_id = asMap.remove('owner_id');
      if (reminder.id != null) {
        await this._firestore
            .collection('users')
            .document(owner_id)
            .collection('reminders')
            .document(reminder.id)
            .set(asMap);
      } else {
        await this._firestore
            .collection('users')
            .document(owner_id)
            .collection('reminders')
            .add(asMap);
      }
    }
  }

  String getUserFromId(String serviceName, String user_id) {
    final who = _services[serviceName]
        .userKeys
        .entries
        .firstWhere((entry) => entry.value == user_id, orElse: () {
      print('No matching users');
      // FIXME: Exception?
      return MapEntry('','');
    });
    return who.key;
  }

  // Callback: - Service name, username => id?
  // Any notifiers anywhere with this username?
  bool checkUser(String serviceName, String username) {
    for (var user in _notifiers.entries) {
      for (var notifier in user.value.entries) {
        if (notifier.value.settings.username == username) {
          return true;
        }
      }
    }
    return false;
  }

  // Update user content, should we check whats in userData??
  Future<bool> updateUser(String serviceName, String userId, Map<String,dynamic> userData) async {
     await this._firestore.collection('users').document(userId).update(userData);
  }

  // callback, setup new user: from service, service username, login user/pass
  Future<bool> createUser(String serviceName, String username, Map loginData) async {
    final isValid = EmailValidator.validate(loginData['email']) &&
    loginData['password'].length >= 6;

    if (!isValid) {
      print('Email or password invalid');
      return false;
    }
    final newUser = await this._fbAuth.signUp(loginData['email'], loginData['password']);
    print('Signed up new user: ${newUser.id}');
    if (newUser != null) {
      // FIXME: Here we now need a new Notifier() or similar
      // Once created (and saved) the next loadNotifiers will setup the userMap for us
      // or we can set it manually here:
      // Create user in firestore:
      await this._firestore.collection('users').document(newUser.id).set({});
      // Add to service details:
      _services[serviceName].userKeys[username] = newUser.id;
      // Create a notifier for the service that created the user
      var name = serviceName.split('.'); // Engine.Telegram
      var notifier = Notifier.fromFirebase(
          {
            'engine': serviceName,
            'settings': '{"name": "${name[1]}", "username": "${username}" }',
          },
        newUser.id,
        _services[serviceName]
      );
      // and store it (we could probably skip the "make a notifier object step" really)
      await this._firestore.collection('users').document(newUser.id).collection('notifiers').add(
      {'engine': serviceName,
      'settings': notifier.settings.toString(),
      'last_modified': notifier.last_modified.toString() });
      return true;
    }
    return false;
  }

  // Callback:
  List<Reminder> getReminderList(String serviceName, String user_id) {
    if(_reminders.isEmpty) {
      return [];
    }
    final sortedReminders =
    _reminders.where((r) => r.owner_id == user_id).toList();
    sortedReminders
        .sort((r1, r2) => r1.next_time.compareTo(r2.next_time));
//    int counter = 1;
//    List<Reminder> reminderList = sortedReminders
//        .map((reminder) => '${counter++}: ${reminder.displayString()}').toList();
//    print(reminderList);
    return sortedReminders.toList();
  }

  // Poll services for any incoming queries
  // void respondToQueries() {
  //   // test:
  //   // _services['Engine.Telegram'].sendMessage('castaway', 'testid', 'blahblah');
  //   _services.forEach((name, service) {
  //     // finished Reminder ids:
  //     Map queries = service.incomingCommands;
  //
  //     for (var user_id in queries.keys) {
  //       for (var command in queries[user_id]) {
  //         MapEntry who = service.userKeys.entries
  //             .firstWhere((entry) => entry.value == user_id, orElse: () {
  //           print('No matching users');
  //           return null;
  //         });
  //
  //         // Using the id of the incoming message as the key to follow any responses
  //         if (command['text'] == '/reminders') {
  //           print('Responding...');
  //           final sortedReminders =
  //               _reminders.where((r) => r.owner_id == user_id).toList();
  //           sortedReminders
  //               .sort((r1, r2) => r1.next_time.compareTo(r2.next_time));
  //           int counter = 1;
  //           String reminderStr = sortedReminders
  //               .map((reminder) => '${counter++}: ${reminder.displayString()}')
  //               .join('\n');
  //           print(reminderStr);
  //           // print('Send: $reminderStr to ${who.key}, id: ${command['id']}');
  //           Map reminderList = {
  //             'text': reminderStr,
  //             'create_buttons':
  //                 List.generate(counter, (index) => '/edit ${index + 1}'),
  //           };
  //           service.sendMessage(
  //               who.key, command['id'].toString(), reminderList);
  //         }
  //         if (command['text'].startsWith('/edit')) {
  //           // how to edit a reminder..
  //         }
  //       }
  //     }
  //
  //     service.clearCommands();
  //   }); // _services.forEach
  // }

  void logStatus(Map log, String userId) async {
    await this._firestore
        .collection('users')
        .document(userId)
        .collection('logs')
        .add(Map.from(log));
  }
}

