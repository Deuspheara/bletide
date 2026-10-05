// package:web exposes only BluetoothUUID. Minimal modern JS interop follows
// Web Bluetooth IDL; navigator and secure-context access use package:web.
@JS()
library;

import 'dart:js_interop';

import 'package:web/web.dart' as web;

extension type BluetoothNavigator(web.Navigator _) implements JSObject {
  external BrowserBluetooth? get bluetooth;
}

extension type BrowserBluetooth(JSObject _) implements JSObject {
  external JSPromise<BrowserDevice> requestDevice(BrowserDeviceOptions options);
  @JS('getAvailability')
  external JSFunction? get availabilityQuery;
  external JSPromise<JSBoolean> getAvailability();
  external void addEventListener(String type, JSFunction listener);
  external void removeEventListener(String type, JSFunction listener);
}

extension type BrowserAvailabilityEvent(JSObject _) implements JSObject {
  external bool get available;
}

extension type BrowserDevice(JSObject _) implements JSObject {
  external String get id;
  external String? get name;
  external BrowserGatt? get gatt;
  external void addEventListener(String type, JSFunction listener);
  external void removeEventListener(String type, JSFunction listener);
}

extension type BrowserDeviceOptions._(JSObject _) implements JSObject {
  external factory BrowserDeviceOptions({
    JSArray<BrowserDeviceFilter>? filters,
    bool? acceptAllDevices,
    JSArray<JSString>? optionalServices,
  });
}

extension type BrowserDeviceFilter._(JSObject _) implements JSObject {
  external factory BrowserDeviceFilter({
    String? namePrefix,
    JSArray<JSString>? services,
  });
}

extension type BrowserError(JSObject _) implements JSObject {
  external String? get name;
  external String? get message;
}

extension type BrowserGatt(JSObject _) implements JSObject {
  external bool get connected;
  external JSPromise<BrowserGatt> connect();
  external void disconnect();
  external JSPromise<JSArray<BrowserService>> getPrimaryServices();
}

extension type BrowserService(JSObject _) implements JSObject {
  external String get uuid;
  external bool get isPrimary;
  external JSPromise<JSArray<BrowserCharacteristic>> getCharacteristics();
}

extension type BrowserCharacteristic(JSObject _) implements JSObject {
  external String get uuid;
  external BrowserProperties get properties;
  external JSDataView? get value;
  external JSPromise<JSDataView> readValue();
  external JSPromise<JSAny?> writeValueWithResponse(JSUint8Array value);
  external JSPromise<JSAny?> writeValueWithoutResponse(JSUint8Array value);
  external JSPromise<JSArray<BrowserDescriptor>> getDescriptors();
  external JSPromise<BrowserCharacteristic> startNotifications();
  external JSPromise<BrowserCharacteristic> stopNotifications();
  external void addEventListener(String type, JSFunction listener);
  external void removeEventListener(String type, JSFunction listener);
}

extension type BrowserDescriptor(JSObject _) implements JSObject {
  external String get uuid;
  external JSPromise<JSDataView> readValue();
  external JSPromise<JSAny?> writeValue(JSUint8Array value);
}

extension type BrowserProperties(JSObject _) implements JSObject {
  external bool get read;
  external bool get write;
  external bool get writeWithoutResponse;
  external bool get notify;
  external bool get indicate;
}
