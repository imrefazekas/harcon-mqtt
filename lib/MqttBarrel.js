const { promisify } = require('util')

let _ = require('isa.js')
let mqtt = require('mqtt')
let Harcon = require('harcon')
let Barrel = Harcon.Barrel
let Communication = Harcon.Communication

let Cerobee = require('clerobee')
let clerobee = new Cerobee( 16 )

let Proback = require('proback.js')

const SEPARATOR = '/'
const REPLACER = '_'
const REPLACER_REGEX = /\_/g
const COLLECTIVE_NODEID = 'COLLS'

function conformiser ( division, name, nodeID ) {
	return division.replace( /\./g, REPLACER ) + SEPARATOR + name.replace( /\./g, REPLACER ) + SEPARATOR + nodeID
}

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
	return new Promise( (resolve, reject) => {
		try {
			let status = JSON.parse( message )

			if (!status.domain || !status.entity || !status.nodeID ) return

			if ( !self.presences[ status.domain ] )
				self.presences[ status.domain ] = {}
			if ( !self.presences[ status.domain ][ status.entity ] )
				self.presences[ status.domain ][ status.entity ] = {}

			self.presences[ status.domain ][ status.entity ][ self.nodeID ] = Date.now()
			resolve('ok')
		} catch (err) { reject( err ) }
	} )
}
mqttbarrel.processHarcon = function ( message ) {
	let self = this
	return new Promise( async (resolve, reject) => {
		try {
			let comm = JSON.parse( message.toString() )

			let reComm = Communication.importCommunication( comm.comm )
			let reResComm = comm.response ? (comm.responseComms.length > 0 ? Communication.importCommunication( comm.responseComms[0] ) : reComm.twist( self.systemFirestarter.name, comm.err ) ) : null

			let interested = (!reResComm && self.matching( reComm ).length !== 0) || (reResComm && self.matchingResponse( reResComm ).length !== 0)

			if ( !interested ) return false
			await self.innerProcessMqtt( comm )
			resolve('ok')
		} catch (err) { reject(err) }
	} )
}
mqttbarrel.extendedInit = function ( config ) {
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
		self.client.on('error', function (err) {
			reject(err)
		})
		self.client.on('connect', function () {
			self.logger.harconlog( null, 'MQTT connection is made.', self.connectURL, 'info' )
			if (!self.outs)
				self.outs = {}
			if (!self.ins)
				self.ins = {}

			called++
			if (called === 0)
				resolve( 'ok' )
		})

		self.client.on('message', async (topic, message) => {
			let routing = topic.split( SEPARATOR )
			let domain = routing.slice(0, routing.length - 2).join('.').replace( REPLACER_REGEX, '.' )
			let entity = routing[ routing.length - 2 ].replace( REPLACER_REGEX, '.' )
			let seq = routing[ routing.length - 1 ]

			if ( !self.ins[ domain ] || !self.ins[ domain ][ entity ] ) return

			if ( seq === COLLECTIVE_NODEID ) {
				return await self.processPresence( message )
			}
			else if ( seq !== self.nodeID ) return

			await self.processHarcon( message )
		} )
		if ( self.timeout > 0 ) {
			self.cleaner = setInterval( function () {
				self.cleanupMessages()
			}, self.timeout )
		}
	} )
}

mqttbarrel.newDivision = function ( division ) {
	var self = this
	return new Promise( (resolve, reject) => {
		if ( !self.outs[division] )
			self.outs[division] = true
		resolve( 'ok' )
	} )
}
mqttbarrel.removeEntity = function ( division, context, name ) {
	var self = this
	return new Promise( (resolve, reject) => {
		if ( self.ins[division] ) {
			self.ins[division][context] = true
			self.ins[division][name] = true
		}
		resolve( 'ok' )
	} )
}
mqttbarrel.newEntity = function ( division, context, name ) {
	var self = this

	return new Promise( (resolve, reject) => {
		if ( !self.ins[division] ) self.ins[division] = []

		if (context && !self.ins[division][context] ) {
			let collTopicName = conformiser( division, context, COLLECTIVE_NODEID )
			let topicName = conformiser( division, context, self.nodeID )

			self.client.subscribe( collTopicName )
			console.log('SUBSCRIBE::: ', topicName )
			self.client.subscribe( topicName )
			self.ins[division][context] = true
		}
		if (!self.ins[division][name] ) {
			let collTopicName = conformiser( division, name, COLLECTIVE_NODEID )
			let topicName = conformiser( division, name, self.nodeID )

			self.client.subscribe( collTopicName )
			console.log('SUBSCRIBE::: ', topicName )
			// REPLACER
			self.client.subscribe( topicName )
			self.ins[division][name] = true
		}
		resolve( 'ok' )
	} )
}

mqttbarrel.innerProcessMqtt = function ( comm ) {
	let self = this

	self.logger.harconlog( null, 'Received from bus...', comm, 'trace' )

	return new Promise( async (resolve, reject) => {
		try {
			let realComm = Communication.importCommunication( comm.comm )
			realComm.nodeID = comm.nodeID || self.nodeID
			if ( !comm.response ) {
				if ( comm.callback )
					realComm.callback = function () { }
				self.logger.harconlog( null, 'Request received from bus...', realComm, 'trace' )
				await self.parentIntoxicate( realComm )
			} else {
				if ( self.messages[ comm.id ] ) {
					realComm.callback = self.messages[ comm.id ].callback
					delete self.messages[ comm.id ]
				}
				let responses = comm.responseComms.map(function (c) { return Communication.importCommunication( c ) })
				await self.parentAppease( realComm, comm.err ? new Error(comm.err) : null, responses )
			}
			resolve('ok')
		} catch (err) { reject(err) }
	} )
}

mqttbarrel.parentAppease = mqttbarrel.appease
mqttbarrel.appease = function ( comm, err, responseComms ) {
	let self = this
	if ( !comm.expose && self.isSystemEvent( comm.event ) )
		return this.parentAppease( comm, err, responseComms )

	return new Promise( async (resolve, reject) => {
		if ( !self.outs[ comm.division ] )
			return reject( new Error('Division is not ready yet: ' + comm.division) )

		try {
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
			let topicName = conformiser( comm.sourceDivision, entityName, nodeNO )

			self.client.publish( topicName, packet )
			resolve('ok')
		} catch (err) { reject(err) }
	} )
}

mqttbarrel.parentIntoxicate = mqttbarrel.intoxicate
mqttbarrel.intoxicate = function ( comm ) {
	let self = this
	if ( self.isSystemEvent( comm.event ) ) return this.parentIntoxicate( comm )

	return new Promise( async (resolve, reject) => {
		if ( !self.outs[ comm.division ] )
			return reject( new Error('Division is not ready yet: ' + comm.division) )

		self.logger.harconlog( null, 'Intoxicating to bus...', comm, 'trace' )

		if ( self.messages[ comm.id ] )
			return reject( new Error('Duplicate message delivery!'), comm.id )

		if ( comm.callback )
			self.messages[ comm.id ] = { callback: comm.callback, timestamp: Date.now() }

		try {
			let entityName = comm.event.substring(0, comm.event.lastIndexOf('.') )
			let packet = JSON.stringify( {
				id: comm.id,
				comm: comm,
				response: false,
				nodeID: self.nodeID,
				callback: !!comm.callback
			} )
			let nodeNO = comm.nodeID || self.randomNodeID( comm.valve, comm.division, entityName )
			let topicName = conformiser( comm.division, entityName, nodeNO )
			self.client.publish( topicName, packet )
			resolve('ok')
		} catch (err) { reject(err) }
	} )
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
				let topicName = conformiser( domain, entity, COLLECTIVE_NODEID )
				self.client.publish( topicName, JSON.stringify( { domain: domain, entity: entity, nodeID: self.nodeID } ) )
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

mqttbarrel.extendedClose = function ( ) {
	var self = this
	return new Promise( async (resolve, reject) => {
		self.clearReporter()
		if ( self.cleaner )
			clearInterval( self.cleaner )
		try {
			if ( self.client )
				self.client.end( true, () => {
					resolve('ok')
				} )
			else resolve('ok')
		} catch (err) { reject(err) }
	} )
}

module.exports = MqttBarrel
