extern crate redis;
use redis::*;
use std::ptr::null;
use crate::domain::consumer_config::*;
pub struct RedisService{}


impl RedisService{

    pub fn get_data(data_key : &str, batch : u32, mut client : Client) -> redis::RedisResult<Vec<String>>{
        let mut conn = client.get_connection().unwrap();
        let scr_str : String  = String::from("local i = tonumber(ARGV[2])\n") + "local res = {}\n"
            + "local length = redis.call('llen',ARGV[1])\n" + "if length < i then i = length end\n"
            + "if i > 0 then\n" + "    res = redis.call('lrange', ARGV[1], -i, -1)\n"
            + "    i=i+1\n" + "    redis.call('ltrim', ARGV[1], 0, -i)\n" + "end\n" + "return res\n";
        let script : Script = redis::Script::new(&scr_str);

        let result: Vec<String> = script.arg(data_key).arg(batch).invoke(&mut conn)?;
        Ok(result)
    }

    pub fn get_config(indices : Vec<&str>) -> Vec<ConsumerConfig>{
        let client = redis::Client::open("redis://10.95.134.109:6379").unwrap();
        let mut conn = client.get_connection().unwrap();
        let mut consumer_configs : Vec<ConsumerConfig> = Vec::new();

        for index_name in indices {
            let data_key : String = conn.hget(index_name, "data_key").unwrap();
            let batch : String = conn.hget(index_name, "batch").unwrap();
            let batch= batch.parse::<u32>().unwrap();
            let thread_num : String = conn.hget(index_name, "thread_num").unwrap();
            let thread_num= thread_num.parse::<u32>().unwrap();

            println!("index name: {}, key: {}", index_name, data_key);
            println!("index name: {}, batch: {}", index_name, batch);
            println!("index name: {}, thread_num: {}", index_name, thread_num);
            consumer_configs.push(ConsumerConfig::build_consumer_config(index_name.to_string(), data_key, batch, thread_num));
        }
        consumer_configs
    }
}
