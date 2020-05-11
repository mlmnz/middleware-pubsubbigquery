'use strict';

const Buffer = require('safe-buffer').Buffer;
const BigQuery = require('@google-cloud/bigquery');
const projectId = "mmmi-209323";
const datasetId = "parking";
const tableId = "raw_data";

const bigquery = new BigQuery({
    projectId: projectId,
});

/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.js, and executed when
 * the trigger topic receives a message.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} callback The callback function.
 */
exports.pubsub2bigQuery = (event, callback) => {

    // Get data from message
  	const payload = event.data
    const context = event.context
	const attributes = payload.attributes;
    const encodeMessage = payload.data;      
    const deviceId = attributes['deviceId'];
    const timestamp = context['timestamp'];
	const messageStr = Buffer.from(encodeMessage,'base64');
  	const message = JSON.parse(messageStr);

    const data = {
        ts: timestamp,
        deviceId: deviceId,
        distance: message.distance,
        rssi: message.rssi,
        voltLevel: message.voltLevel
    };
  
  
    // Send to BigQuery  
        bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert(data)
        .then(() => {
            console.log(`Inserted ${data.length} rows`);
          })
        .catch(err => {
          if (err && err.name === 'PartialFailureError') {
            if (err.errors && err.errors.length > 0) {
              console.log('Insert errors:');
              err.errors.forEach(err => console.error(err));
            }
          } else {
            console.error('ERROR:', err);
          }
         });

    callback();
};