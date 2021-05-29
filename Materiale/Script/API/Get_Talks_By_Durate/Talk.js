const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    _id: String,
    main_speaker: String,
    title: String,
    details: String,
    posted: String,
    url: String,
    num_views: Number,
    durate_sec: Number,
    tags: Array
}, { collection: 'tedx_data' });

module.exports = mongoose.model('talk', talk_schema);