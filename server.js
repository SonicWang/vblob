/*
  Additional modules: express
*/
var Logger = require('./common/logger').Logger; //logging module
var valid_name = require('./common/bucket_name_check').is_valid_name; //bucket name check
var express = require("express"); //express web framework
var s3auth = require('./common/s3-auth'); //front end authentication (s3 style)
var j2x = require('./common/json2xml'); //json to xml transformation
var util = require('util');
var fs = require('fs');
var events = require('events');
var drivers = { }; //storing backend driver objects
var driver_order = { }; //give sequential numbering for drivers
var current_driver = null; //current driver in use
var argv = process.argv;
var conf_file = "./config.json";
var XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/";
var credential_hash = { };
for (var idx = 0; idx < argv.length; idx++)
{
  if (argv[idx] === "-f" && idx+1 < argv.length)
    { conf_file = argv[idx+1]; }
}

var config;
try
{
  config  = JSON.parse(fs.readFileSync(conf_file));
} catch (err)
{
  console.error("error:"+(new Date())+" - Reading configuration error: " + err);
  return;
}

if (config.keyID && config.secretID) { credential_hash[config.keyID] = config.secretID; }

var logger = new Logger(config.logtype, config.logfile);

var app = express.createServer( );
var server_ready = new events.EventEmitter();
server_ready.pending_dr = 1; //one driver at any time

server_ready.on('start', function() {
  logger.info(('listening to port ' + config.port));
  if (config.port)
  { app.listen(parseInt(config.port,10));}
});

logger.info(('starting server'));
var driver_start_callback = function (key) {
  return  function (obj) {
    obj.driver_key = key;
    console.log('driver initialization done for '+key);
    server_ready.pending_dr--;
    if (server_ready.pending_dr === 0) server_ready.emit('start');
  };
};

(function() {
  var drs = config.drivers;
  for (var i = 0, len = drs.length; i < len; ++i) {
    var dr = drs[i];
    var key = Object.keys(dr)[0];
    var value = dr[key];
    driver_order[key] = i;
    value.option.logger = logger;
    if (config.current_driver !== undefined) {
      if (config.current_driver.toLowerCase() === key) {current_driver = drivers[key]=require('./drivers/'+value.type).createDriver(value.option, driver_start_callback(key) );}
    } else
    if (current_driver === null) {
      current_driver = drivers[key] = require('./drivers/'+value.type).createDriver(value.option, driver_start_callback(key) );
    }
  }
}());

var hdr_case_conv_table = {"last-modified":"Last-Modified", "accept-ranges":"Accept-Ranges", "content-range":"Content-Range",
"content-length":"Content-Length", "content-type":"Content-Type",
"content-encoding":"Content-Encoding", "content-disposition":"Content-Disposition",
"expires":"Expires", "cache-control":"Cache-Control"};
// for compatibility, ensure that some response headers match S3 exactly (even though HTTP headers should be case insensitive)
var normalize_resp_headers = function (headers,method, code, body, stream) {
  if (headers.etag) {
    headers.ETag = headers.etag;
    delete headers.etag;
  }
  headers.Connection = "close";
  if (headers.connection) { headers.Connection = headers.connection; delete headers.connection; }
  var keys = Object(hdr_case_conv_table);
  for (var idx = 0; idx < hdr_case_conv_table.length; idx++)
    if (headers[keys[idx]]) { headers[hdr_case_conv_table[keys[idx]]] = headers[keys[idx]]; delete headers[keys[idx]]; }
  if (!body && !stream && method !== 'head') {//no response payload, no type
    if (headers["Content-Type"]) delete headers["Content-Type"];
    //check if it's 204, if no add 0
    if (code !== 204) {
      headers["Content-Length"] = 0;
    }
  }
  if (body || code === 204) { //xml response, not content-length
    if (headers["Content-Length"]) delete headers["Content-Length"];
  }
  if (headers.date) { headers.Date = headers.date; delete headers.date; }
  if (!headers.Date) { headers.Date = new Date().toUTCString(); }
  if (headers.server) { headers.Server = headers.server; delete headers.server; }
  if (!headers.Server) { headers.Server = "Blob Service"; }
  if (!headers["x-amz-request-id"]) headers["x-amz-request-id"] = "1D2E3A4D5B6E7E8F9"; //No actual request id for now
  if (!headers["x-amz-id-2"]) headers["x-amz-id-2"] = "3F+E1E4B1D5A9E2DB6E5E3F5D8E9"; //no actual request id 2
}

var general_resp = function (res,post_proc,verb) {//post_proc is for post-processing response body
  return function (resp_code, resp_header, resp_body, resp_data) {
    if (res.client_closed) { return; }
    var headers = resp_header;
    var xml_body = "";
    if (resp_body) {
      if (resp_code < 300 && post_proc) resp_body = post_proc(resp_body); //make sure not to process error response
      xml_body = j2x.json2xml(resp_body,0,resp_code >= 300?undefined:XMLNS);
      if (headers["content-type"]) delete headers["content-type"];
      headers["Content-Type"] = "application/xml";
    }
    normalize_resp_headers(headers, verb, resp_code, resp_body !== null, resp_data !== null);
    res.writeHeader(resp_code,headers);
    if (resp_body && verb !== 'head') {
      res.write(xml_body);
    }
    if (resp_data && verb !== 'head') {
      //need to stream out
      resp_data.pipe(res);
    } else res.end();
  };
};

var authenticate = function(req,res,next) {
  var Authorization = req.headers.authorization;
  var targets = {};
  if (req.params && req.params.bucket) { targets.bucket = req.params.bucket; }
  if (req.params && req.params[0]) { targets.filename = req.params[0]; }
  targets.query = req.query;
  var res_body;
  if (config.auth === 'enabled') {
    //only do authentication if enabled
    var key = null;
    if (Authorization) {
      //extract key
      key = Authorization.substring(4, Authorization.indexOf(':'));
    }
    if (!key || !credential_hash[key] || s3auth.validate(key, credential_hash[key], req.method.toUpperCase(), targets, req.headers, Authorization) === false ) {
      general_resp(res,null,req.method.toLowerCase())(401,{},{Error:{Code:"Unauthorized",Message:"Signature does not match"}}, null);
      return;
    }
  }
  if (targets.bucket && !valid_name(targets.bucket)) {
    logger.error(('Invalid bucket name: ' + targets.bucket));
      general_resp(res,null,req.method.toLowerCase())(400,{},{Error:{Code:"InvalidBucketName",Message:"The specified bucket is not valid"}}, null);
    return;
  }
  next();
};


if (config.debug) {
  express.logger.token('headers', function(req, res){ return '\n' + req.method + ' ' + req.url + '\n' + util.inspect(req.headers) + '\n\n' + res._header + '\n'; })
  app.use(express.logger(':headers'));
}

var bucket_list_post_proc = function(resp_body) {
  if (resp_body.ListAllMyBucketsResult.Owner === undefined) {
    resp_body.ListAllMyBucketsResult.Owner = {ID : "1a2b3c4d5e6f7" , DisplayName : "blob" } ; //inject arbitrary owner info
  }
  return resp_body;
};

app.get('/',authenticate);
app.get('/',function(req,res) {
  if (req.method === 'HEAD') { //not allowed
    general_resp(res,null,'head')(405,{'Allow':'GET'},null, null);
    return;
  }
  current_driver.bucket_list(general_resp(res,bucket_list_post_proc));
});


app.get('/:bucket[/]{0,1}$', authenticate);
app.get('/:bucket[/]{0,1}$',function(req,res) {
  res.client_closed = false;
  req.connection.addListener('close', function () {
    res.client_closed = true;
  });
  var opt = {};
  if (req.query.marker) { opt.marker = req.query.marker; }
  if (req.query.prefix) { opt.prefix = req.query.prefix; }
  if (req.query.delimiter) { opt.delimiter = req.query.delimiter; }
  if (req.query["max-keys"]) { opt["max-keys"] = req.query["max-keys"]; }
  //if (req.query.location !== undefined) { opt.location = req.query.location; }
  //if (req.query.logging !== undefined) { opt.logging = req.query.logging; }
  current_driver.object_list(req.params.bucket,opt,general_resp(res,null,req.method.toLowerCase()));
});

var get_hdrs = [ 'if-modified-since','if-unmodified-since', 'if-match', 'if-none-match'];
var get_qrys = [ 'response-content-type', 'response-content-language', 'response-expires',
'response-cache-control', 'response-content-disposition', 'response-content-encoding'];
app.get('/:bucket/*',authenticate);
app.get('/:bucket/*',function(req,res) {
  res.client_closed = false;
  req.connection.addListener('close', function () {
    res.client_closed = true;
  });
  var options = {}, idx;
  for (idx = 0; idx < get_qrys.length; idx++)
    if (req.query[get_qrys[idx]]) options[get_qrys[idx]] = req.query[get_qrys[idx]];
  for (idx = 0; idx < get_hdrs.length; idx++)
    if (req.headers[get_hdrs[idx]]) options[get_hdrs[idx]] = req.headers[get_hdrs[idx]];
  if (req.headers.range) options.range = req.headers.range;
  options.method = req.method.toLowerCase();
  current_driver.object_read(req.params.bucket, req.params[0], options,general_resp(res,null,options.method));
});

app.put('/:bucket[/]{0,1}$', authenticate);
app.put('/:bucket[/]{0,1}$',function(req,res) {
  //always empty option for now
  current_driver.bucket_create(req.params.bucket,{},req,general_resp(res));
});

var put_hdrs = [ 'cache-control', 'content-disposition', 'content-encoding', 'content-length',
'content-type', 'expires'];
var put_opts = ['content-md5','x-amz-storage-class'];
var copy_hdrs = [ 'x-amz-copy-source-if-match', 'x-amz-copy-source-if-none-match',
'x-amz-copy-source-if-unmodified-since', 'x-amz-copy-source-if-modified-since',
'x-amz-metadata-directive', 'x-amz-storage-class'];
app.put('/:bucket/*', authenticate);
app.put('/:bucket/*', function(req,res) {
  var metadata = {}, options = {}, idx;
  for (idx = 0; idx < put_hdrs.length; idx++)
    if (req.headers[put_hdrs[idx]]) metadata[put_hdrs[idx]] = req.headers[put_hdrs[idx]];
  var keys = Object.keys(req.headers);
  for (idx = 0; idx < keys.length; idx++) {
    if (keys[idx].match(/^x-amz-meta-/)) metadata[keys[idx]] = req.headers[keys[idx]];
  }
  keys = null;
  if (req.headers['x-amz-copy-source'] ) {
    var src = req.headers['x-amz-copy-source'];
    var src_buck = src.slice(1,src.indexOf('/',1));
    var src_obj = src.substr(src.indexOf('/',1)+1);
    for (idx = 0; idx < copy_hdrs.length; idx++)
      if (req.headers[copy_hdrs[idx]]) options[copy_hdrs[idx]] = req.headers[copy_hdrs[idx]];
    current_driver.object_copy(req.params.bucket, req.params[0], src_buck, src_obj, options, metadata, general_resp(res));
  } else {
    for (idx = 0; idx < put_opts.length; idx++)
      if (req.headers[put_opts[idx]]) options[put_opts[idx]] = req.headers[put_opts[idx]];
    current_driver.object_create(req.params.bucket,req.params[0],options,metadata,req,general_resp(res));
  }
});

app.delete('/:bucket[/]{0,1}$', authenticate);
app.delete('/:bucket[/]{0,1}$',function(req,res) {
  current_driver.bucket_delete(req.params.bucket,general_resp(res));
});

app.delete('/:bucket/*',authenticate);
app.delete('/:bucket/*',function(req,res) {
  current_driver.object_delete(req.params.bucket,req.params[0],general_resp(res));
});

exports.vblob_gateway = app;
