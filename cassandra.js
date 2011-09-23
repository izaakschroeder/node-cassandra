
var 
	sys = require('sys'),
	EventEmitter = require("events").EventEmitter,
	Proxy = require('proxy'),
	Thrift = require('thrift'),
	Cassandra = require('Cassandra'),
	CassandraTypes = require('cassandra_types');
	


var Connection = exports.Connection = function() {
	
	var self = this;
	
	if (arguments.length < 1)
		throw "Must pass hostname:port to this class!";
	
	EventEmitter.call(this);
		
	var servers = [];
	
	this.currentKeyspace = null;
	
	for (var i in arguments) {
		var host = arguments[i], port = 9160;
		var parts = host.split(":");
		if (parts.length == 2) {
			host = parts[0];
			port = parts[1];
		}
		servers.push({
			host: host, port: port
		});
	}
	
	
	var connection, client;
	
	function activate() {
		var server = servers.pop();
		connection = Thrift.createConnection(server.host, server.port),
		/*connection.on("error", function(err) {
			console.log("Could not connect to "+server.host+":"+server.port+": "+err+".");
			if (servers.length > 0) {
				activate();
			}
			else {
				console.log("Tried all servers, could not connect!");
			}
		});*/
		connection.on("connect", function() {
			self.client = client = Thrift.createClient(Cassandra, this);
			self.emit("connect");
		})
	}
	
	activate();
	
}
sys.inherits(Connection, EventEmitter);

Connection.prototype.keyspace = function(name) {
	return new Keyspace(this, name);
}

Connection.prototype.setKeyspace = function(name, done) {
	if (name == this.currentKeyspace) {
		if (done)
			done();
		return;
	}
	
	var self = this;
	this.client.set_keyspace(name, function(err){
		if (!err) {
			self.currentKeyspace = name;
			if (done)
				done();
		}
	});
}


var Keyspace = this.Keyspace = function(connection, name) {
	this.connection = connection;
	this.name = name;
}

Keyspace.prototype.columnFamily = function(name) {
	return new ColumnFamily(this, name);
}

Keyspace.prototype.activate = function(cb) {
	this.connection.setKeyspace(this.name, cb);
	return this;
}

Keyspace.prototype.describe = function(cb) {
	var self = this;
	this.connection.client.describe_keyspace(this.name, function(err, res) {
		if (err instanceof CassandraTypes.NotFoundException) {
			if (cb)
				cb(null);
		} 
		else if (!err) {
			self.replicationCount = res.replication_factor
			self.replicationStrategy =  res.strategy_class;
			self.replicationOptions = res.strategy_options;
			self.columnFamilies = { };
			for (var i in res.cf_defs) {
				var cf = res.cf_defs[i];
				var obj = new ColumnFamily(self, cf.name);
				obj.type = cf.comparator_type;
				obj.subType = cf.subcomparator_type;
				self.columnFamilies[obj.name] = obj;
			}
			
			if (cb)
				cb(self);
		}
		else {
			console.log(err);
			return;
		}
	})
	return this;
}

Keyspace.prototype.create = function(opts, cb) {
	if (typeof opts === "function") {
		cb = opts;
		opts = { };
	}
	var opts = Object.extend({
		replicationCount: 1,
		replicationStrategy: "org.apache.cassandra.locator.SimpleStrategy"
	}, opts);
	this.connection.client.system_add_keyspace(new CassandraTypes.KsDef({
		replication_factor: opts.replicationCount,
		strategy_class: opts.replicationStrategy,
		name: this.name,
		cf_defs: []
	}), function(err){
		if (err) {
			console.log(err);
			return;
		}
		
		for(var i in opts)
			this[i] = opts[i];
		
		if (cb)
			cb(self);
	});
	return this;
}

Keyspace.prototype.remove = function(cb) {
	this.connection.client.system_drop_keyspace(this.name, function(err){
		if (!err)
			if (cb)
				cb();
	});
	return null;
}

Keyspace.prototype.exists = function(cb) {
	this.describe(function(ks){
		if (cb)
			cb(ks !== null);
	})
}

Keyspace.prototype.materialize = function(opts, cb) {
	var self = this;
	if (typeof opts === "function") {
		cb = opts;
		opts = { };
	}
	this.describe(function(result) {
		if (!result) {
			self.create(opts, cb);
		}
		else {
			if (opts)
				for (var i in opts)
					if (self[i] != opts[i])
						throw "Mismatched keyspace!";
			if (cb)
				cb();
		}
	})
	return this;
}

var ColumnFamily = function(keyspace, name) {
	this.connection = keyspace.connection;
	this.keyspace = keyspace;
	this.name = name;
}

ColumnFamily.prototype.key = function(key) {
	var res = this.keys(key);
	res.use = key;
	return res;
}

ColumnFamily.prototype.keys = function() {
	var args = arguments.length == 1 && Array.isArray(arguments[0]) ? arguments[0] : Array.prototype.slice.call(arguments);
	return new KeySet(this, args);
}

ColumnFamily.prototype.describe = function(cb) {
	var self = this;
	this.keyspace.describe(function(res) {
		var d = res.columnFamilies[self.name];
		for (var i in d)
			self[i] = d[i];
		if (cb)
			cb(d ? self : null);
	});
}

ColumnFamily.prototype.create = function(opts, cb) {
	var self = this;
	this.connection.client.system_add_column_family(new CassandraTypes.CfDef({
		keyspace: self.keyspace.name,
		name: self.name,
		column_type: !opts.subType ? 'Standard' : 'Super',
		comparator_type: opts.type,
		subcomparator_type: opts.subType
	}), function(err){
		if (err) {
			console.log(err);
			return;
		}
		
		for (var i in opts)
			self[i] = opts[i];
		
		if (cb)
			cb(self);
	});
}

ColumnFamily.prototype.remove = function(cb) {
	this.connection.client.system_drop_column_family(this.name, function(err) {
		
	});
}

ColumnFamily.prototype.exists = function() {
	
}

ColumnFamily.prototype.materialize = function(type, subType, cb) {
	var self = this;
	
	if (typeof subType === "function") {
		cb = subType;
		subType = undefined;
	}
	
	if (typeof type === "function") {
		cb = type;
		opts = { 
			type: 'BytesType'
		}
	} else if (typeof type === "object") {
		opts = type;
	} else {
		opts = {
			type: type || 'BytesType',
			subType: subType
		}
	}
		
	if (opts.type.indexOf('.') == -1)
		opts.type = 'org.apache.cassandra.db.marshal.' + opts.type;
	if (opts.subType && opts.subType.indexOf('.') == -1)
		opts.subType = 'org.apache.cassandra.db.marshal.' + opts.subType;
	
	this.describe(function(result){
		if (!result) {
			self.create(opts, cb);
		}
		else {
			if (opts)
				for (var i in opts)
					if (self[i] != opts[i])
						throw "Mismatched column family on "+i+"( required: "+opts[i]+" != self: "+self[i]+")!";
			if (cb)
				cb(self);
		}
	});
	
	return this;
}


var KeySet = this.KeySet = function(columnFamily, keys) {
	this.connection = columnFamily.connection;
	this.columnFamily = columnFamily;
	this.keys = keys;
}

KeySet.prototype.property = function(property) {
	var res = this.properties(property);
	res.use = property;
	return res;
}

KeySet.prototype.properties = function(properties) {
	var args = arguments.length == 1 && Array.isArray(arguments[0]) ? arguments[0] : Array.prototype.slice.call(arguments);
	return new PropertySet(this, args);
}

KeySet.prototype.exists = function(done) {
	var self = this;
	this.properties({ limit: 1 }).get(function(result){
		function isEmpty(ob){
			for(var i in ob)
				if (ob.hasOwnProperty(i))
					return false;
			return true;
		}
		done(self.use ? !isEmpty(result) : !isEmpty(result[self.keys[0]]));
	})
}


var PropertySet = function(keySet, values, parent) {
	this.connection = keySet.connection;
	this.values = values;
	this.parent = parent;
	this.keySet = keySet;
}

PropertySet.prototype.property = function(prop) {
	var res = this.properties(prop);
	res.use = prop;
	return res;
}

PropertySet.prototype.properties = function() {
	var args = arguments.length == 1 && Array.isArray(arguments[0]) ? arguments[0] : Array.prototype.slice.call(arguments);
	return new PropertySet(this.keySet, args, this);
}

PropertySet.prototype.get = function(done) {
	var propertySet = this;
	propertySet.connection.setKeyspace(propertySet.keySet.columnFamily.keyspace.name, function(){

		var columnParent = new CassandraTypes.ColumnParent({
			column_family: propertySet.keySet.columnFamily.name,
			super_column: (typeof propertySet.parent !== "undefined") ? propertySet.parent.values[0] : undefined
		});
		
		var predicates = [];
		var columnNames = [];
		for (var i in propertySet.values) {
			var value = propertySet.values[i];
			if (typeof value === "object")
				predicates.push(new CassandraTypes.SlicePredicate({
					slice_range: new CassandraTypes.SliceRange({
						start: value.start || '',
						finish: value.end || '',
						reversed: value.reversed || false,
						count: value.limit || 100
					})
				}));
			else
				columnNames.push(value);
		}
		
		if (columnNames.length > 0)
			predicates.push(new CassandraTypes.SlicePredicate({
				column_names: propertySet.values
			}))
		
		var consistency = CassandraTypes.ConsistencyLevel.QUORUM;
		
		var out = {};
		var remaining = predicates.length;
		
		for (var i in predicates) {
			var predicate = predicates[i];
			propertySet.connection.client.multiget_slice(propertySet.keySet.keys, columnParent, predicate, consistency, function(err, result){
				if (err) {
					console.log("Error: "+err);
					return;
				}

				
				for(var k in propertySet.keySet.keys) {
					var key = propertySet.keySet.keys[k];
					var data = result[key];
					
					if (typeof out[key] === "undefined")
						out[key] = { };
					
					if (data.length == 0) 
						continue;
					
					
					if (data[0].column)
						for (var i in data)
							out[key][data[i].column.name] = data[i].column.value;
					else
						for (var i in data) {
							var column = data[i].super_column;
							for(var j in column.columns) {
								var subColumn = column.columns[j];
								if (typeof out[key][column.name] === "undefined")
									out[key][column.name] = { }
								out[key][column.name][subColumn.name] = subColumn.value;
							}
						}
				}
				
				if (--remaining == 0) {
					
					if (propertySet.keySet.use)
						out = out[propertySet.keySet.use];
				
					if (propertySet.parent && propertySet.parent.use)
						out = out[propertySet.parent.use];	
					
					if (propertySet.use)
						out = out[propertySet.use];
					
					done(out);
				}
			})
		}
		
		
	});
}

PropertySet.prototype.set = function(vals, done) {
	
	var propertySet = this;
	var consistency = CassandraTypes.ConsistencyLevel.QUORUM;
	var columns, superColumn;
	var timestamp = Math.round(Date.now()/1000);
	
	superColumns = [];
	columns = [];
		
	
	if (typeof vals === "object") {
		
		if (typeof propertySet.parent !== "undefined")
			throw "DERP";
		
		var localColumns = [];
		for(var v in vals) {
			var name = v;
			var value = vals[v];
			localColumns.push(new CassandraTypes.Column({
				name: name,
				value: value,
				timestamp: timestamp
			}));
		}
		
		for (var v in propertySet.values) {
			var name = propertySet.values[v];
			superColumns.push(new CassandraTypes.SuperColumn({
				name: name,
				columns: localColumns
			}));
		}
		
	} else {
		for (var v in propertySet.values) {
			var name = propertySet.values[v];
			columns.push(new CassandraTypes.Column({
				name: name,
				value: vals,
				timestamp: timestamp
			}));
		}
		
		if (typeof propertySet.parent !== "undefined") {
			superColumns = [new CassandraTypes.SuperColumn({
				name: propertySet.parent.values[0],
				columns: columns
			})];
			columns = [];
		}
	}
	
	
	
	
	propertySet.connection.setKeyspace(propertySet.keySet.columnFamily.keyspace.name, function(){
		
		mutations = { };
		
		for(var k in propertySet.keySet.keys) {
			var key = propertySet.keySet.keys[k];
			mutations[key] = { };
			var local = mutations[key][propertySet.keySet.columnFamily.name] = [];
			for (var i in columns)
				local.push(new CassandraTypes.Mutation({
					column_or_supercolumn: new CassandraTypes.ColumnOrSuperColumn({
						column: columns[i]
					})
				}));
			for (var i in superColumns)
				local.push(new CassandraTypes.Mutation({
					column_or_supercolumn: new CassandraTypes.ColumnOrSuperColumn({
						super_column: superColumns[i]
					})
				}));
		}
		
		
		//console.log(require('util').inspect(mutations, false, 5));
		
		propertySet.connection.client.batch_mutate(mutations, consistency, function(err) {
			if (err) {
				console.log("Error: "+err)
				return;
			}
			
			if (done)
				done();
			
		});
	});
	
}

PropertySet.prototype.delete = function() {
	
}

