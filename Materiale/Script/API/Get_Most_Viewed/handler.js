const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talk = require('./Talk');

module.exports.get_most_viewed = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
	
    // set default
	if (!body.doc_per_page) {
        body.doc_per_page = 10
    }
    if (!body.page) {
        body.page = 1
    }
    
    connect_to_db().then(() => {
        console.log('=> get the id');
		if(!body.year) 		
				talk.find({},{_id: 1, main_speaker: 1, title: 1, details: 1, posted: 1, url: 1, n_views: 1, durate_sec: 1, tags: 1})
				.sort({n_views: -1})
				.skip((body.doc_per_page * body.page) - body.doc_per_page)
				.limit(body.doc_per_page)
				.then(talks => {
						callback(null, {
							statusCode: 200,
							body: JSON.stringify(talks)
						})
					}
				)
				.catch(err =>
					callback(null, {
						statusCode: err.statusCode || 500,
						headers: { 'Content-Type': 'text/plain' },
						body: 'Could not fetch the id.'
					})
				);

		else if (body.year && !body.month) {
				data_min=body.year+"-01"
				data_max=body.year+"-12"
				
				talk.find({posted: {$gte: data_min , $lte: data_max}},{_id: 1, main_speaker: 1, title: 1, details: 1, posted: 1, url: 1, n_views: 1, durate_sec: 1, tags: 1})
				.sort({n_views: -1})
				.skip((body.doc_per_page * body.page) - body.doc_per_page)
				.limit(body.doc_per_page)
				.then(talks => {
						callback(null, {
							statusCode: 200,
							body: JSON.stringify(talks)
						})
					}
				)
				.catch(err =>
					callback(null, {
						statusCode: err.statusCode || 500,
						headers: { 'Content-Type': 'text/plain' },
						body: 'Could not fetch the id part 2.'
					})
				);
		}
		else 
				talk.find({posted: {$eq: body.year+"-"+body.month}},{_id: 1, main_speaker: 1, title: 1, details: 1, posted: 1, url: 1, n_views: 1, durate_sec: 1, tags: 1})
				.sort({n_views: -1})
				.skip((body.doc_per_page * body.page) - body.doc_per_page)
				.limit(body.doc_per_page)
				.then(talks => {
						callback(null, {
							statusCode: 200,
							body: JSON.stringify(talks)
						})
					}
				)
				.catch(err =>
					callback(null, {
						statusCode: err.statusCode || 500,
						headers: { 'Content-Type': 'text/plain' },
						body: 'Could not fetch the id part 2.'
					})
				);
    });
};