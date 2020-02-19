let _ = require('isa.js')
let mqtt = require('mqtt')
let Harcon = require('harcon')
let Barrel = Harcon.Barrel
let Communication = Harcon.Communication

let Cerobee = require('clerobee')
let clerobee = new Cerobee( 16 )

const SEPARATOR = '/'
const REPLACER = '_'
const REPLACER_REGEX = /\_/g
const COLLECTIVE_NODEID = 'COLLS'

let Proback = require('proback.js')

const fs = require('fs')
const path = require('path')
let VERSION = exports.VERSION = JSON.parse( fs.readFileSync( path.join( process.cwd(), 'package.json'), 'utf8' ) ).version

function conformiser ( division, name, nodeID ) {
	return division.replace( /\./g, REPLACER ) + SEPARATOR + name.replace( /\./g, REPLACER ) + SEPARATOR + nodeID
}

let errorAttribs = [ 'code', 'message', 'errorCode', 'id', 'reason', 'params' ]
function printError (err) {
	let res = {}
	for (let attrib of errorAttribs)
		if ( err[ attrib ] )
			res[ attrib ] = err[attrib]

	res.message = err.message || err.toString()
	return res
}
function buildError (obj) {
	let error = new Error('')
	for (let attrib of errorAttribs)
		if ( obj[ attrib ] )
			error[ attrib ] = obj[attrib]

	return error
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

mqttbarrel.processPresence = async function ( message ) {
	let self = this

	let status = JSON.parse( message )

	if ( !status.divisions && (!status.division || !status.entity || !status.nodeID) ) return

	if ( status.divisions ) {
		for (let division of status.divisions)
			await self.extendedNewDivision( division )
		return 'ok'
	}

	if ( !self.presences[ status.domain ] )
		self.presences[ status.domain ] = {}
	if ( !self.presences[ status.domain ][ status.entity ] )
		self.presences[ status.domain ][ status.entity ] = {}

	self.presences[ status.domain ][ status.entity ][ self.nodeID ] = {
		timestamp: Date.now(),
		warper: self.warper.inpose( status.warper ),
		projectVersion: status.projectVersion,
		entityVersion: status.entityVersion
	}
	return 'ok'
}
mqttbarrel.processHarcon = async function ( message ) {
	let self = this
	let comm = JSON.parse( message.toString() )

	let reComm = Communication.importCommunication( comm.comm )
	let reResComm = comm.response ? (comm.responseComms.length > 0 ? Communication.importCommunication( comm.responseComms[0] ) : reComm.twist( self.systemFirestarter.name, comm.err ) ) : null

	let interested = (!reResComm && self.matching( reComm ).length !== 0) || (reResComm && self.matchingResponse( reResComm ).length !== 0)

	if ( !interested ) return false
	await self.innerProcessMqtt( comm )
	return 'ok'
}
mqttbarrel.extendedInit = function ( config = {} ) {
	let self = this

	let called = -1
	return new Promise( (resolve, reject) => {
		self.messages = {}

		self.ignorePresense = !!config.ignorePresense

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

mqttbarrel.extendedNewDivision = async function ( division ) {
	var self = this
	if ( !self.outs[division] )
		self.outs[division] = true
	return 'ok'
}
mqttbarrel.extendedRemoveEntity = async function ( division, context, name ) {
	var self = this
	if ( self.ins[division] ) {
		self.ins[division][context] = true
		self.ins[division][name] = true
	}
	return 'ok'
}
mqttbarrel.extendedNewEntity = async function ( division, context, name ) {
	var self = this

	if ( !self.ins[division] ) self.ins[division] = []

	if (context && !self.ins[division][context] ) {
		let collTopicName = conformiser( division, context, COLLECTIVE_NODEID )
		let topicName = conformiser( division, context, self.nodeID )

		self.client.subscribe( collTopicName )
		self.logger.harconlog( null, 'SUBSCRIBED TO ' + topicName, null, 'info' )
		self.client.subscribe( topicName )
		self.ins[division][context] = true
	}
	if (!self.ins[division][name] ) {
		let collTopicName = conformiser( division, name, COLLECTIVE_NODEID )
		let topicName = conformiser( division, name, self.nodeID )

		self.client.subscribe( collTopicName )
		self.logger.harconlog( null, 'SUBSCRIBED TO ' + topicName, null, 'info' )
		// REPLACER
		self.client.subscribe( topicName )
		self.ins[division][name] = true
	}
	return 'ok'
}

mqttbarrel.innerProcessMqtt = async function ( comm ) {
	let self = this

	self.logger.harconlog( null, 'Packet Received', comm, 'trace' )

	let realComm = Communication.importCommunication( comm.comm )
	realComm.nodeID = comm.nodeID || self.nodeID
	if ( !comm.response ) {
		if ( comm.callback )
			realComm.callback = function () { }
		await self.appease( realComm )
	} else {
		if ( self.messages[ comm.id ] ) {
			realComm.callback = self.messages[ comm.id ].callback
			delete self.messages[ comm.id ]
		}
		let responses = comm.responseComms.map(function (c) { return Communication.importCommunication( c ) })
		await self.appease( realComm, comm.err ? buildError(comm.err) : null, responses )
	}
	return 'ok'
}

mqttbarrel.intoxicateAnswer = async function ( comm, err, responseComms ) {
	let self = this
	if ( !comm.expose && self.isSystemEvent( comm.event ) )
		return this.appease( comm, err, responseComms )

	if ( !self.outs[ comm.division ] )
		throw new Error('Division is not ready yet: ' + comm.division)

	let entityName = comm.source // event.substring(0, comm.event.indexOf('.') )
	let error = err ? printError(err) : null
	let packet = JSON.stringify( {
		id: comm.id,
		comm: comm,
		nodeID: self.nodeID,
		err: error,
		response: true,
		responseComms: responseComms || []
	} )

	self.logger.harconlog( null, 'Packet sending', {comm: comm, err: error, responseComms: responseComms}, 'trace' )
	let nodeNO = comm.sourceNodeID || self.randomNodeID( comm.valve, comm.sourceDivision, entityName )
	let topicName = conformiser( comm.sourceDivision, entityName, nodeNO )

	self.client.publish( topicName, packet )
	return 'ok'
}

mqttbarrel.intoxicateMessage = async function ( comm ) {
	let self = this
	if ( self.isSystemEvent( comm.event ) )
		return this.appease( comm )

	if ( !self.outs[ comm.division ] )
		throw new Error('Division is not ready yet: ' + comm.division)

	if ( self.messages[ comm.id ] )
		throw new Error('Duplicate message delivery: ' + comm.id )

	self.logger.harconlog( null, 'Packet sending', comm, 'trace' )

	if ( comm.callback )
		self.messages[ comm.id ] = { callback: comm.callback, timestamp: Date.now() }

	let entityName = comm.event.substring(0, comm.event.lastIndexOf('.') )
	let packet = JSON.stringify( {
		id: comm.id,
		comm: comm,
		response: false,
		nodeID: self.nodeID,
		callback: !!comm.callback
	} )

	if (!this.ignorePresense && (!this.presences || !this.presences[ comm.division ] || !this.presences[ comm.division ][ entityName ]) ) {
		await Proback.until( () => {
			return self.presences && self.presences[ comm.division ] && self.presences[ comm.division ][ entityName ]
		}, 10, self.reporterInterval * 2 )
	}

	let nodeNO = self.randomNodeID( comm.valve, comm.division, entityName )
	let topicName = conformiser( comm.division, entityName, nodeNO )
	self.client.publish( topicName, packet )
	return 'ok'
}

mqttbarrel.checkPresence = function ( ) {
	let self = this

	let timestamp = Date.now()
	Object.keys(self.presences).forEach( function (division) {
		Object.keys(self.presences[division]).forEach( function (entity) {
			Object.keys(self.presences[division][entity]).forEach( function (nodeID) {
				if ( self.presences[division][entity][nodeID] <= timestamp - self.keeperInterval )
					delete self.presences[division][entity][nodeID]
			} )
		} )
	} )
}

mqttbarrel.reportStatus = function ( ) {
	let self = this

	try {
		let divisions = Object.keys(self.ins)

		let mainTopicName = conformiser( self.division, '*', COLLECTIVE_NODEID )
		self.client.publish( mainTopicName, JSON.stringify( { divisions: divisions } ) )

		divisions.forEach( function (division) {
			Object.keys(self.ins[division]).forEach( function (entity) {
				let topicName = conformiser( division, entity, COLLECTIVE_NODEID )
				self.client.publish( topicName, JSON.stringify( {
					division: division,
					entity: entity,
					nodeID: self.nodeID,
					projectVersion: VERSION,
					entityVersion: entity.version || '1.0.0'
				} ) )
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
		if ( self.keeper )
			clearInterval( self.keeper )
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
