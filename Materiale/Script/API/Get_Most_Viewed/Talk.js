const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    main_speaker: String,
    title: String,
    details: String,
    posted: String,
    url: String,
    num_views: String,
    durate: String,
    tags: Array
}, { collection: 'tedx_data' });

module.exports = mongoose.model('talk', talk_schema);