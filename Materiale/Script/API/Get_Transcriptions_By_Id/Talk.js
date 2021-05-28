const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    _id: String,
    trascription: Array
}, { collection: 'tedx_data' });

module.exports = mongoose.model('talk', talk_schema);