'use strict'

var Sequelize = require('sequelize')
const config = require('./dbCon.json')

const options = {
  logging: false,
  maxConcurrentQueries: 100,
  native: false,
  define: {
    timestamps: false, // Extra columns in tables for created and udpated
    underscored: true
  },
  pool: { maxConnections: 10, maxIdleTime: 30 }
}

try {
  options.native = !!require('pg-native')
} catch (err) {
  // if pg-native can't be found, use the non-native version
}

module.exports = new Sequelize(config.connectionString, options)
