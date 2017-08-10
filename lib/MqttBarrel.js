'use strict'

let _ = require('isa.js')
let mqtt = require('mqtt')
let Harcon = require('harcon')
let Barrel = Harcon.Barrel
let Communication = Harcon.Communication

let Cerobee = require('clerobee')
let clerobee = new Cerobee( 16 )

let Proback = require('proback.js')

const SEPARATOR = '/'
const COLLECTIVE_NODEID = 'COLLS'

function MqttBarrel ( ) { }
MqttBarrel.prototype = new Barrel()
let mqttbarrel = MqttBarrel.prototype

mqttbarrel.randomNodeID = function ( valve, division, entityName ) {
	if ( _.isNumber( valve ) ) return valve

	if ( !this.presences || !this.presences[division] || !this.presences[division][entityName] ) return this.nodeID

	let ids = Object.keys( this.presences[division][entityName] )
	let id = ids[ Math.floor( Math.random( ) * ids.length ) ]
	return id
}

mqttbarrel.processPresence = function ( message ) {
	let self = this
	try {
		let status = JSON.parse( message )

		if (!status.domain || !status.entity || !status.nodeID ) return

		if ( !self.presences[ status.domain ] )
			self.presences[ status.domain ] = {}
		if ( !self.presences[ status.domain ][ status.entity ] )
			self.presences[ status.domain ][ status.entity ] = {}

		self.presences[ status.domain ][ status.entity ][ self.nodeID ] = Date.now()
	} catch (err) { self.logger.harconlog( err ) }
}
mqttbarrel.processHarcon = function ( message ) {
	let self = this
	try {
		let comm = JSON.parse( message.toString() )

		let reComm = Communication.importCommunication( comm.comm )
		let reResComm = comm.response ? (comm.responseComms.length > 0 ? Communication.importCommunication( comm.responseComms[0] ) : reComm.twist( self.systemFirestarter.name, comm.err ) ) : null

		let interested = (!reResComm && self.matching( reComm ).length !== 0) || (reResComm && self.matchingResponse( reResComm ).length !== 0)

		if ( !interested ) return false
		self.innerProcessMqtt( comm )
	} catch (err) { self.logger.harconlog(err) }
}
mqttbarrel.extendedInit = function ( config, callback ) {
	let self = this

	let called = -1
	return new Promise( (resolve, reject) => {
		self.messages = {}

		self.nodeID = clerobee.generate()
		self.reporterInterval = config.reporterInterval || 2000
		self.reporter = setInterval( () => { self.reportStatus() }, self.reporterInterval )
		self.presences = {}
		self.keeperInterval = config.keeperInterval || 3000
		self.keeper = setInterval( () => { self.checkPresence() }, self.keeperInterval )

		self.connectURL = config.connectURL || 'mqtt://localhost'
		self.timeout = config.timeout || 0

		self.client = mqtt.connect( self.connectURL )
		self.client.on('connect', function () {
			self.logger.harconlog( null, 'MQTT connection is made.', self.connectURL, 'info' )
			if (!self.outs)
				self.outs = {}
			if (!self.ins)
				self.ins = {}

			called++
			if (called === 0)
				Proback.resolver( 'ok', callback, resolve )
		})

		self.client.on('message', function (topic, message) {
			let routing = topic.split( SEPARATOR )

			var domain = routing.slice(0, routing.length - 2).join('.'), entity = routing[ routing.length - 2 ], seq = routing[ routing.length - 1 ]
			if ( !self.ins[ domain ] || !self.ins[ domain ][ entity ] ) return

			if ( seq === COLLECTIVE_NODEID ) {
				return self.processPresence( message )
			}
			else if ( seq !== self.nodeID ) return

			self.processHarcon( message )
		} )
		if ( self.timeout > 0 ) {
			self.cleaner = setInterval( function () {
				self.cleanupMessages()
			}, self.timeout )
		}
	} )
}

mqttbarrel.newDivision = function ( division, callback ) {
	if ( !this.outs[division] )
		this.outs[division] = true
	return Proback.quicker( 'ok', callback )
}
mqttbarrel.removeEntity = function ( division, context, name, callback) {
	return Proback.quicker( 'ok', callback )
}
mqttbarrel.newEntity = function ( division, context, name, callback) {
	var self = this

	return new Promise( (resolve, reject) => {
		if ( !self.ins[division] ) self.ins[division] = []

		if (context && !self.ins[division][context] ) {
			self.client.subscribe( division + SEPARATOR + context + SEPARATOR + COLLECTIVE_NODEID )
			console.log('SUBSCRIBE::: ', division + SEPARATOR + context + SEPARATOR + self.nodeID )
			self.client.subscribe( division + SEPARATOR + context + SEPARATOR + self.nodeID )
			self.ins[division][context] = true
		}
		if (!self.ins[division][name] ) {
			self.client.subscribe( division + SEPARATOR + name + SEPARATOR + COLLECTIVE_NODEID )
			console.log('SUBSCRIBE::: ', division + SEPARATOR + name + SEPARATOR + self.nodeID )
			self.client.subscribe( division + SEPARATOR + name + SEPARATOR + self.nodeID )
			self.ins[division][name] = true
		}
		Proback.resolver( 'ok', callback, resolve )
	} )
}

mqttbarrel.innerProcessMqtt = function ( comm ) {
	let self = this

	self.logger.harconlog( null, 'Received from bus...', comm, 'trace' )

	let realComm = Communication.importCommunication( comm.comm )
	realComm.nodeID = comm.nodeID || 1

	if ( !comm.response ) {
		// console.log( comm.callback )
		if ( comm.callback )
			realComm.callback = function () { }
		self.logger.harconlog( null, 'Request received from bus...', realComm, 'trace' )
		self.parentIntoxicate( realComm )
	} else {
		if ( self.messages[ comm.id ] ) {
			realComm.callback = self.messages[ comm.id ].callback
			delete self.messages[ comm.id ]
		}
		let responses = comm.responseComms.map(function (c) { return Communication.importCommunication( c ) })

		self.parentAppease( realComm, comm.err ? new Error(comm.err) : null, responses )
	}
}

mqttbarrel.parentAppease = mqttbarrel.appease
mqttbarrel.appease = function ( comm, err, responseComms ) {
	let self = this
	if ( !comm.expose && self.isSystemEvent( comm.event ) )
		return this.parentAppease( comm, err, responseComms )

	if ( !self.outs[ comm.division ] )
		return self.logger.harconlog( new Error('Division is not ready yet: ' + comm.division) )

	let entityName = comm.source // event.substring(0, comm.event.indexOf('.') )
	let packet = JSON.stringify( {
		id: comm.id,
		comm: comm,
		nodeID: self.nodeID,
		err: err ? err.message : null,
		response: true,
		responseComms: responseComms || []
	} )

	self.logger.harconlog( null, 'Appeasing...', {comm: comm, err: err ? err.message : null, responseComms: responseComms}, 'trace' )
	let nodeNO = comm.nodeID || self.randomNodeID( comm.valve, comm.sourceDivision, entityName )
	self.client.publish( comm.sourceDivision + SEPARATOR + entityName + SEPARATOR + nodeNO, packet )
}

mqttbarrel.parentIntoxicate = mqttbarrel.intoxicate
mqttbarrel.intoxicate = function ( comm ) {
	let self = this
	if ( self.isSystemEvent( comm.event ) ) return this.parentIntoxicate( comm )

	if ( !self.outs[ comm.division ] )
		return self.logger.harconlog( new Error('Division is not ready yet: ' + comm.division) )

	self.logger.harconlog( null, 'Intoxicating to bus...', comm, 'trace' )

	if ( self.messages[ comm.id ] )
		return self.logger.harconlog( new Error('Duplicate message delivery!'), comm.id )

	// console.log( '\n\n', comm.event, self.messages )
	if ( comm.callback )
		self.messages[ comm.id ] = { callback: comm.callback, timestamp: Date.now() }
	// console.log( '\n\n', comm.event, comm.division, self.messages )

	let entityName = comm.event.substring(0, comm.event.indexOf('.') )
	let packet = JSON.stringify( {
		id: comm.id,
		comm: comm,
		response: false,
		nodeID: self.nodeID,
		callback: !!comm.callback
	} )
	let nodeNO = comm.nodeID || self.randomNodeID( comm.valve, comm.division, entityName )
	self.client.publish( comm.division + SEPARATOR + entityName + SEPARATOR + nodeNO, packet )
}

mqttbarrel.checkPresence = function ( ) {
	let self = this

	let timestamp = Date.now()
	Object.keys(self.presences).forEach( function (domain) {
		Object.keys(self.presences[domain]).forEach( function (entity) {
			Object.keys(self.presences[domain][entity]).forEach( function (nodeID) {
				if ( self.presences[domain][entity][nodeID] <= timestamp - self.keeperInterval )
					delete self.presences[domain][entity][nodeID]
			} )
		} )
	} )
}

mqttbarrel.reportStatus = function ( ) {
	let self = this

	try {
		Object.keys(self.ins).forEach( function (domain) {
			Object.keys(self.ins[domain]).forEach( function (entity) {
				self.client.publish( domain + SEPARATOR + entity + SEPARATOR + COLLECTIVE_NODEID, JSON.stringify( { domain: domain, entity: entity, nodeID: self.nodeID } ) )
			} )
		} )
	} catch ( err ) { self.logger.harconlog( err ) }
}

mqttbarrel.clearReporter = function ( ) {
	if (this.reporter) {
		clearInterval( this.reporter )
		this.reporter = null
	}
}

mqttbarrel.extendedClose = function ( callback ) {
	var self = this
	return new Promise( (resolve, reject) => {
		self.clearReporter()
		if ( self.cleaner )
			clearInterval( self.cleaner )
		if ( self.client )
			self.client.end( true, Proback.handler( callback, resolve, reject ) )
	} )
}

module.exports = MqttBarrel
