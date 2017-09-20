const mongoose = require('mongoose')

mongoose.Promise = global.Promise

const schema = new mongoose.Schema({
  provider: {
    type: String,
    required: true,
    lowercase: true
  },
  identity: {
    type: String,
    required: true,
    unique: true
  },
  userId: {
    type: String,
    required: true
  },
  accessToken: {
    type: String,
    required: true
  },
  refreshToken: {
    type: String
  },
  profile: {
    type: mongoose.Schema.Types.Mixed
  },
  createdAt: {
    type: Date,
    default: Date.now
  }
})

schema.set('toJSON', {virtuals: true})
schema.set('toObject', {virtuals: true})

module.exports = mongoose.model('Service', schema)
