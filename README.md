# harcon-mqtt
AMQP transport layer ("Barrel") plugin for [harcon](https://github.com/imrefazekas/harcon).

Both PUB/SUB and PUSH/PULL socket types are supported. See socket types explained [here](http://www.squaremobius.net/rabbit.js/).


## Installation

```javascript
npm install harcon harcon-mqtt --save
```


## Usage

```javascript
var Harcon = require('harcon');
var Mqtt = require('harcon-mqtt');

var mqttConfig = {
	connectURL: 'mqtt://localhost',
	timeout: 0
};
var harcon = new Harcon( { Barrel: Mqtt.Barrel, barrel: mqttConfig }, function(err){
} );
```

Should the recipients be not available or fail to meet the timeframe defined by the attribute 'timeout', [harcon](https://github.com/imrefazekas/harcon) will call the callbacks of the given messages with an error.

[![js-standard-style](https://cdn.rawgit.com/feross/standard/master/badge.svg)](https://github.com/feross/standard)
