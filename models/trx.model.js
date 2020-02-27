const mongoose = require('mongoose')
const Schema = mongoose.Schema

let TrxSchema = new Schema({
	  value: { type : String },
	  offset: {type: Number},
	  partition: {type: Number},
	  highWaterOffset: {type: String},
	  key: {type: String}
	})

module.exports = mongoose.model('Trx', TrxSchema)
