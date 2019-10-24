/**
 * @fileoverview Functions implementing Twitter's realtime filtered stream
 * @author Joey Whelan <joey.whelan@gmail.com>
 */

'use strict';
'use esversion 6';
const fetch = require('node-fetch');
const btoa = require('btoa');
const AbortController = require('abort-controller');

const ABORT_TIMEOUT = 90; //time, in seconds, to abort a streaming connection
const CONSUMER_KEY = process.env.CONSUMER_KEY;  //twitter auth key
const CONSUMER_SECRET = process.env.CONSUMER_SECRET;  //twitter auth secret
const AUTH_URL   = 'https://api.twitter.com/oauth2/token';  //url for fetching a twitter bearer token
const RULES_URL  = 'https://api.twitter.com/labs/1/tweets/stream/filter/rules';
const STREAM_URL = 'https://api.twitter.com/labs/1/tweets/stream/filter?format=compact';
const RULES = [{'value' : 'from:realDonaldTrump -is:retweet -is:quote'}];
let g_reader;
let g_backoff = 0;

/**
 * Function that clears out existing rules on a twitter realtime filter
 * @param {string} token - twitter bearer token
 * @param {string} url - url to the twitter filter rules API
 * @return {promise} integer representing number of rules deleted
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function clear(token, url) {
    console.debug(`${(new Date()).toISOString()} clearAllRules()`);

    const rules = await getRules(token, url);  //fetch all the existing rules
    if (Array.isArray(rules.data)) {
        const ids = rules.data.map(rule => rule.id);
        const deleted = await deleteRules(token, ids, url);  //delete them
        return deleted;
    }
    else {
        return 0;
    }
}

/**
 * Function that deletes an array of ids on a twitter realtime filter
 * @param {string} token - twitter bearer token
 * @param {Array} ids - array rule ids to delete
 * @param {string} url - url to the twitter filter rules API
 * @return {promise} integer representing number of rules deleted
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function deleteRules(token, ids, url) {
    console.debug(`${(new Date()).toISOString()} deleteRules()`);
 
    const body = {
        'delete' : {
            'ids': ids
        }
    };
    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type' : 'application/json',
                'Authorization' : 'Bearer ' + token
            },
            body: JSON.stringify(body)
        });
        if (response.ok) {
            const json = await response.json();
            return json.meta.summary.deleted;
        }
        else {
            throw new Error(`response status: ${response.status} ${response.statusText}`);    
        }
    }
    catch (err) {
        console.error(`${(new Date()).toISOString()} deleteRules() - ${err}`);
        throw err;
    }
}

/**
 * Main function.  Fetches a twitter bearer token, clears out any existing filter rules, adds
 * new rules, and then sets up a realtime tweet feed for those new rules.
 */
async function filter() {
    console.debug(`${(new Date()).toISOString()} filter()`);
    try {
        const token = await getTwitterToken(AUTH_URL);
        const deleted = await clear(token, RULES_URL);
        console.log(`${(new Date()).toISOString()} number of rules deleted: ${deleted}`);
        const added = await setRules(token, RULES, RULES_URL);
        console.log(`${(new Date()).toISOString()} number of rules added: ${added}`);
        await stream(token, STREAM_URL);
    }
    catch(err) {
        console.error(`${(new Date()).toISOString()} filter() exiting`);
        g_reader.removeAllListeners();
        process.exit(-1);
    }
}

/**
 * Function that fetches the existing set of filter rules
 * @param {string} token - twitter bearer token
 * @param {string} url - url to the twitter filter rules API
 * @return {promise} json object containing an array of existing filter rule ids
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function getRules(token, url) {
    console.debug(`${(new Date()).toISOString()} getRules()`);
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: {
            'Authorization' : 'Bearer ' + token
            }
        });
        if (response.ok) {
            const json = await response.json();
            return json;
        }
        else {
            throw new Error(`response status: ${response.status} ${response.statusText}`);    
        }
    }
    catch (err) {
        console.error(`${(new Date()).toISOString()} getRules() - ${err}`);
        throw err;
    }
}

/**
* Fetches an app-only bearer token via Twitter's oauth2 interface
* @param {string} url- URL to Twitter's OAuth2 interface
* @return {string} - Bearer token
*/
async function getTwitterToken(url) {
    console.debug(`${(new Date()).toISOString()} getTwitterToken()`);
    const consumerToken = btoa(urlEncode(CONSUMER_KEY) + ':' + urlEncode(CONSUMER_SECRET));

    let response, json;

    try {
        response = await fetch(url, {
            method: 'POST',
            headers: {
                'Authorization' : 'Basic ' + consumerToken,
                'Content-Type' : 'application/x-www-form-urlencoded;charset=UTF-8'
            }, 
            body : 'grant_type=client_credentials'
        });

        if (response.ok) {
            json = await response.json();
            return json.access_token;
        }
        else {
            throw new Error(`response status: ${response.status}`);
        }
    }
    catch (err) {
        console.error(`${(new Date()).toISOString()} getTwitterToken() - ${err}`)
        throw err;
    } 
} 

/**
 * Function that adds filter rules to an account
 * @param {string} token - twitter bearer token
 * @param {Array} rules - array of filter rules to add
 * @param {string} url - url to the twitter filter rules API
 * @return {promise} integer representing number of rules added
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function setRules(token, rules, url) {
    console.debug(`${(new Date()).toISOString()} setRules()`);
 
    const body = {'add' : rules};
    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type'  : 'application/json',
                'Authorization' : 'Bearer ' + token
            },
            body: JSON.stringify(body)
        });
        if (response.ok) {
            const json = await response.json();
            return json.meta.summary.created;
        }
        else {
            throw new Error(`response status: ${response.status} ${response.statusText}`);    
        }
    }
    catch (err) {
        console.error(`${(new Date()).toISOString()} setRules() - ${err}`);
        throw err;
    }
}

/**
 * Function that performs a call to the twitter filter search API and displays tweets in realtime.
 * @param {string} token - twitter bearer token
 * @param {string} url - url to the twitter stream API
 * @return {promise} none
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function stream(token, url) {
    console.debug(`${(new Date()).toISOString()} stream()`);

    const controller = new AbortController();
    let abortTimer = setTimeout(() => { controller.abort(); }, ABORT_TIMEOUT * 1000);
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: {
            'Authorization' : 'Bearer ' + token
            },
            signal: controller.signal 
        });
        switch (response.status) {
            case 200:
                console.debug(`${(new Date()).toISOString()} stream() - 200 response`);
                g_reader = response.body;
                g_reader.on('data', (chunk) => {
                    try {
                        const json = JSON.parse(chunk);
                        let text = json.data.text.replace(/\r?\n|\r|@|#/g, ' ');  //remove newlines, @ and # from tweet text
                        console.log(`${(new Date()).toISOString()} tweet: ${text}`);
                    }
                    catch (err) {
                        //heartbeat will generate a json parse error.  No action necessary; continue to read the stream.
                        console.debug(`${(new Date()).toISOString()} stream() - heartbeat received`);
                    } 
                    finally {
                        g_backoff = 0;
                        clearTimeout(abortTimer);
                        abortTimer = setTimeout(() => { controller.abort(); }, ABORT_TIMEOUT * 1000);
                    } 
                });
                g_reader.on('error', err => {  //self-induced timeout, immediately reconnect
                    if (err.name === 'AbortError') {
                        console.warn(`${(new Date()).toISOString()} stream() - connection aborted, reconnecting`);
                        g_backoff = 0;
                        clearTimeout(abortTimer);
                        (async () => {await stream(token, url)}) ();
                    }
                    else if (err.code === 'ETIMEDOUT') { //back off 250 ms linearly to 16 sec for connection timeouts
                        console.warn(`${(new Date()).toISOString()} stream() - connection timeout, reconnecting`);
                        g_backoff += .25; 
                        if (g_backoff > 16) {
                            g_backoff = 16;
                        }
                        clearTimeout(abortTimer); 
                        setTimeout(async () => {await stream(token, url)}, g_backoff * 1000);
                    }
                    else {  //fatal error, bail
                        console.error(`${(new Date()).toISOString()} stream() - fatal error ${err}`);
                        clearTimeout(abortTimer);
                        g_reader.removeAllListeners();
                        process.exit(-1);
                    }
                });
                break;
            case 304:  //60 second backoff
                console.debug(`${(new Date()).toISOString()} stream() - 304 response`);
                g_backoff = 60;
                clearTimeout(abortTimer);
                setTimeout(async () => {await stream(token, url)}, g_backoff * 1000);
                break;
            case 420:
            case 429:  //exponential backoff, starting at 60 sec, for rate limit errors
                console.warn(`${(new Date()).toISOString()} stream() - 420, 429 response`);
                if (g_backoff == 0) {
                    g_backoff = 60;
                }
                else {
                    g_backoff = g_backoff * 2;
                }
                clearTimeout(abortTimer);
                setTimeout(async () => {await stream(token, url)}, g_backoff * 1000);
                break;
            case 500:
            case 502:
            case 503:
            case 504: //exponential backoff, starting at 5 sec, for server side errors
                console.warn(`${(new Date()).toISOString()} stream() - 5xx response`);
                if (g_backoff == 0) {
                    g_backoff = 5;
                }
                else {
                    g_backoff *= 2;
                    if (g_backoff > 320) {
                        g_backoff = 320;
                    }
                }
                clearTimeout(abortTimer);  
                setTimeout(async () => {await stream(token, url)}, g_backoff * 1000);
                break;    
            default:  //fail fast for any other errors (4xx client errors, for example)
                clearTimeout(abortTimer);
                g_reader.removeAllListeners();
                throw new Error(`response status: ${response.status}`);
        }
        return;
    }
    catch (err) { //fatal error
        clearTimeout(abortTimer);
        g_reader.removeAllListeners();
        console.error(`${(new Date()).toISOString()} stream() - ${err}`);
        throw err;
    }
}

/**
* Function I found on stackoverflow to provide url encoding
* @param {string} str- string to be encoded
* @return {string} - url encoded string
*/
function urlEncode (str) {
    return encodeURIComponent(str)
        .replace(/!/g, '%21')
        .replace(/'/g, '%27')
        .replace(/\(/g, '%28')
        .replace(/\)/g, '%29')
        .replace(/\*/g, '%2A')
}

filter()
.catch(err => {
    console.error(err);
    g_reader.removeAllListeners();
    process.exit(-1);
});
