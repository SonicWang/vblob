/*
  Filter Driver. Sits on top of other drivers such as fs,s3,or even another filter driver.
  Could inject transformation or control logic south/north bound.
*/
var Filter_Driver = function Filter_Driver(option,callback)
{
  option.option.logger = option.logger; //pass logger into inner config
  this.client = require('../'+option.type).createDriver(option.option,callback);
  this.client.logger = option.logger;
};

Filter_Driver.prototype.container_list = function(callback)
{
  this.client.container_list(callback);
};

Filter_Driver.prototype.container_delete = function(bucket_name,callback)
{
  this.client.container_delete(bucket_name,callback);
};

Filter_Driver.prototype.container_create = function(bucket_name,options,data_stream,callback)
{
  this.client.container_create(bucket_name,options, data_stream,callback);
};

Filter_Driver.prototype.file_delete = function(bucket_name,object_key,callback)
{
  this.client.file_delete(bucket_name,object_key,callback);
};

Filter_Driver.prototype.file_list = function(bucket_name,option,callback)
{
  this.client.file_list(bucket_name,option,callback);
};

Filter_Driver.prototype.file_read = function(bucket_name,object_key,options, callback)
{
  this.client.file_read(bucket_name,object_key,options,callback);
};

Filter_Driver.prototype.file_create = function(bucket_name,object_key,options,metadata,data_stream,callback)
{
  this.client.file_create(bucket_name,object_key, options, metadata, data_stream, callback);
};

Filter_Driver.prototype.file_copy = function(bucket_name, object_key, source_bucket,source_object_key,options,metadata,callback)
{
  this.client.file_copy(bucket_name,object_key, source_bucket, source_object_key, options,metadata,callback);
};

Filter_Driver.prototype.get_config = function() {
  var obj = {};
  obj.type = "filter";
  obj.option = this.client.get_config();
  return obj;
};
module.exports.createDriver = function(option,callback) {
  return new Filter_Driver(option,callback);
};
