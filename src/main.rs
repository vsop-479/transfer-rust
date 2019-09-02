extern crate tantivy;
extern crate time;

use transfer::service::redis_service::*;
use transfer::service::toshi_service::*;
use transfer::domain::consumer_config::*;
use redis::*;
use core::borrow::Borrow;
use std::{thread, fs};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::sync::mpsc;
use std::thread::JoinHandle;
use tantivy::schema::{Schema, STORED, TEXT};
use tantivy::Index;
use tantivy::doc;
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
use transfer::service::tantivy::dns_service::DNSService;
use std::ptr::null;
use transfer::service::tantivy::tantivy_service::TantivyService;
use transfer::service::tantivy::tcpflow_service::TCPFlowService;
use transfer::service::tantivy::weblog_service::WEBLogService;
use tantivy::merge_policy::{LogMergePolicy, MergePolicy, NoMergePolicy};


fn main() {
    let multi: u32 = 3;
    create(multi);
//    start_local();
    start_pro(multi);
}

fn start_local() {
    let buffer = 100_000_000usize;
    let commit_interval = 10;
    let num_threads = 2usize;
    let batch_size = 1000;

    let mut mergePolicy = LogMergePolicy::default();
    mergePolicy.set_level_log_size(0.75);
    mergePolicy.set_min_layer_size(10_000);
    mergePolicy.set_min_merge_size(8);
//    let mergePolicy = NoMergePolicy::default();
    let mergePolicy = Box::new(mergePolicy);

    let mut handles = Vec::new();

    //dns
    handles.push(thread::Builder::new().name("t_dns_1".to_string()).spawn(move || {
        dns("E:\\tantivy_data\\dns", mergePolicy, num_threads.clone(), commit_interval.clone(),
               buffer.clone(), batch_size.clone());
    }).unwrap());

    for handle in handles {
        handle.join().unwrap();
    }
}

fn start_pro(multi: u32 ){
//乘以该因子，判断到达下一个level
    let level_log_size = 0.75;
//    level的跨度，同一个跨度能的segments，属于同一个level.
    let min_layer_size = 200_000_000;
//    最少merge到一起的segments数量.
    let min_merge_size = 8;

    let mut handles= Vec::new();

    for i in 1..multi{
        let _handles = get_handlers(i);
        for handle in _handles{
            handles.push(handle);
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn get_handlers(num: u32) -> Vec<JoinHandle<()>>{
    let buffer = 500_000_000usize;
    let commit_interval = 50;
    let num_threads = 4usize;
    let batch_size = 1000;
    let no_merge = false;

//乘以该因子，判断到达下一个level
    let level_log_size = 0.75;
//    level的跨度，同一个跨度能的segments，属于同一个level.
    let min_layer_size = 200_000_000;
//    最少merge到一起的segments数量.
    let min_merge_size = 8;

    let mut handles = Vec::new();
    //weblog
    handles.push(thread::Builder::new().name("t_weblog_1_".to_string() + &num.to_string()).spawn(move||{
        let mut mergePolicy = LogMergePolicy::default();
        mergePolicy.set_level_log_size(level_log_size.clone());
        mergePolicy.set_min_layer_size(min_layer_size.clone());
        mergePolicy.set_min_merge_size(min_merge_size.clone());
        let mergePolicy = Box::new(mergePolicy.clone());

        if no_merge{
            let mergePolicy = NoMergePolicy::default();
            let mergePolicy = Box::new(mergePolicy.clone());
        }
        weblog(&("/data02/tantivy/weblog".to_string() + &num.to_string()),
               mergePolicy, num_threads.clone(), commit_interval.clone(),
               buffer.clone(), batch_size.clone());
    }).unwrap());
    handles.push(thread::Builder::new().name("t_weblog_2_".to_string() + &num.to_string()).spawn(move||{
        let mut mergePolicy = LogMergePolicy::default();
        mergePolicy.set_level_log_size(level_log_size.clone());
        mergePolicy.set_min_layer_size(min_layer_size.clone());
        mergePolicy.set_min_merge_size(min_merge_size.clone());
        let mergePolicy = Box::new(mergePolicy.clone());

        if no_merge{
            let mergePolicy = NoMergePolicy::default();
            let mergePolicy = Box::new(mergePolicy.clone());
        }
        weblog(&("/data03/tantivy/weblog".to_string() + &num.to_string()), mergePolicy, num_threads.clone(), commit_interval.clone(),
               buffer.clone(), batch_size.clone());
    }).unwrap());
    handles.push(thread::Builder::new().name("t_weblog_3_".to_string() + &num.to_string()).spawn(move||{
        let mut mergePolicy = LogMergePolicy::default();
        mergePolicy.set_level_log_size(level_log_size.clone());
        mergePolicy.set_min_layer_size(min_layer_size.clone());
        mergePolicy.set_min_merge_size(min_merge_size.clone());
        let mergePolicy = Box::new(mergePolicy.clone());

        if no_merge{
            let mergePolicy = NoMergePolicy::default();
            let mergePolicy = Box::new(mergePolicy.clone());
        }
        weblog(&("/data04/tantivy/weblog".to_string() + &num.to_string()), mergePolicy, num_threads.clone(), commit_interval.clone(),
               buffer.clone(), batch_size.clone());
    }).unwrap());

    //tcpflow
    handles.push(thread::Builder::new().name("t_tcpflow_1_".to_string() + &num.to_string()).spawn(move||{
        let mut mergePolicy = LogMergePolicy::default();
        mergePolicy.set_level_log_size(level_log_size.clone());
        mergePolicy.set_min_layer_size(min_layer_size.clone());
        mergePolicy.set_min_merge_size(min_merge_size.clone());
        let mergePolicy = Box::new(mergePolicy.clone());

        if no_merge{
            let mergePolicy = NoMergePolicy::default();
            let mergePolicy = Box::new(mergePolicy.clone());
        }
        tcpflow(&("/data05/tantivy/tcpflow".to_string() + &num.to_string()), mergePolicy, num_threads.clone(), commit_interval.clone(),
                buffer.clone(), batch_size.clone());
    }).unwrap());
    handles.push(thread::Builder::new().name("t_tcpflow_2_".to_string() + &num.to_string()).spawn(move||{
        let mut mergePolicy = LogMergePolicy::default();
        mergePolicy.set_level_log_size(level_log_size.clone());
        mergePolicy.set_min_layer_size(min_layer_size.clone());
        mergePolicy.set_min_merge_size(min_merge_size.clone());
        let mergePolicy = Box::new(mergePolicy.clone());

        if no_merge{
            let mergePolicy = NoMergePolicy::default();
            let mergePolicy = Box::new(mergePolicy.clone());
        }
        tcpflow(&("/data06/tantivy/tcpflow".to_string() + &num.to_string()), mergePolicy, num_threads.clone(), commit_interval.clone(),
                buffer.clone(), batch_size.clone());
    }).unwrap());
    handles.push(thread::Builder::new().name("t_tcpflow_3_".to_string() + &num.to_string()).spawn(move||{
        let mut mergePolicy = LogMergePolicy::default();
        mergePolicy.set_level_log_size(level_log_size.clone());
        mergePolicy.set_min_layer_size(min_layer_size.clone());
        mergePolicy.set_min_merge_size(min_merge_size.clone());
        let mergePolicy = Box::new(mergePolicy.clone());

        if no_merge{
            let mergePolicy = NoMergePolicy::default();
            let mergePolicy = Box::new(mergePolicy.clone());
        }
        tcpflow(&("/data07/tantivy/tcpflow".to_string() + &num.to_string()), mergePolicy, num_threads.clone(), commit_interval.clone(),
                buffer.clone(), batch_size.clone());
    }).unwrap());
    handles.push(thread::Builder::new().name("t_tcpflow_4_".to_string() + &num.to_string()).spawn(move||{
        let mut mergePolicy = LogMergePolicy::default();
        mergePolicy.set_level_log_size(level_log_size.clone());
        mergePolicy.set_min_layer_size(min_layer_size.clone());
        mergePolicy.set_min_merge_size(min_merge_size.clone());
        let mergePolicy = Box::new(mergePolicy.clone());

        if no_merge{
            let mergePolicy = NoMergePolicy::default();
            let mergePolicy = Box::new(mergePolicy.clone());
        }
        tcpflow(&("/data08/tantivy/tcpflow".to_string() + &num.to_string()), mergePolicy, num_threads.clone(), commit_interval.clone(),
                buffer.clone(), batch_size.clone());
    }).unwrap());

    //dns
    handles.push(thread::Builder::new().name("t_dns_1_".to_string() + &num.to_string()).spawn(move||{

        let mut mergePolicy = LogMergePolicy::default();
        mergePolicy.set_level_log_size(level_log_size.clone());
        mergePolicy.set_min_layer_size(min_layer_size.clone());
        mergePolicy.set_min_merge_size(min_merge_size.clone());
        let mergePolicy = Box::new(mergePolicy.clone());

        if no_merge{
            let mergePolicy = NoMergePolicy::default();
            let mergePolicy = Box::new(mergePolicy.clone());
        }
        dns(&("/data09/tantivy/dns".to_string() + &num.to_string()), mergePolicy.clone(), num_threads.clone(), commit_interval.clone(),
            buffer.clone(), batch_size.clone());
    }).unwrap());
    handles.push(thread::Builder::new().name("t_dns_2_".to_string() + &num.to_string()).spawn(move||{
        let mut mergePolicy = LogMergePolicy::default();
        mergePolicy.set_level_log_size(level_log_size.clone());
        mergePolicy.set_min_layer_size(min_layer_size.clone());
        mergePolicy.set_min_merge_size(min_merge_size.clone());
        let mergePolicy = Box::new(mergePolicy.clone());

        if no_merge{
            let mergePolicy = NoMergePolicy::default();
            let mergePolicy = Box::new(mergePolicy.clone());
        }
        dns(&("/data10/tantivy/dns".to_string() + &num.to_string()), mergePolicy.clone(), num_threads.clone(), commit_interval.clone(),
            buffer.clone(), batch_size.clone());
    }).unwrap());
    handles.push(thread::Builder::new().name("t_dns_3_".to_string() + &num.to_string()).spawn(move||{
        let mut mergePolicy = LogMergePolicy::default();
        mergePolicy.set_level_log_size(level_log_size.clone());
        mergePolicy.set_min_layer_size(min_layer_size.clone());
        mergePolicy.set_min_merge_size(min_merge_size.clone());
        let mergePolicy = Box::new(mergePolicy.clone());

        if no_merge{
            let mergePolicy = NoMergePolicy::default();
            let mergePolicy = Box::new(mergePolicy.clone());
        }
        dns(&("/data11/tantivy/dns".to_string() + &num.to_string()), mergePolicy.clone(), num_threads.clone(), commit_interval.clone(),
            buffer.clone(), batch_size.clone());
    }).unwrap());
    handles
}

fn weblog(index_path : &str, mergePolicy: Box<dyn MergePolicy>, num_threads : usize, commit_interval : u32, buffer : usize, batch_size : u32){
    let schema = WEBLogService::get_schema();
    let mut index = TantivyService::open_index(index_path);
    WEBLogService::bulk(mergePolicy, num_threads, index, schema, commit_interval, buffer, batch_size);
}

fn tcpflow(index_path : &str, mergePolicy: Box<dyn MergePolicy>, num_threads : usize, commit_interval : u32, buffer : usize, batch_size : u32){
    let schema = TCPFlowService::get_schema();
    let mut index = TantivyService::open_index(index_path);
    TCPFlowService::bulk(mergePolicy, num_threads, index, schema, commit_interval, buffer, batch_size);
}

fn dns(index_path : &str, mergePolicy: Box<dyn MergePolicy>, num_threads : usize, commit_interval : u32, buffer : usize, batch_size : u32){
    let schema = DNSService::get_schema();
    let mut index = TantivyService::open_index(index_path);
    DNSService::bulk(mergePolicy, num_threads, index, schema, commit_interval, buffer, batch_size);
}

fn create(multi: u32 ){
    for i in 1..multi{
        let index_path = &("/data09/tantivy/dns".to_string() + &i.to_string());
        let schema = DNSService::get_schema();
        let index = TantivyService::create_index(index_path, schema.clone());
        let index_path = &("/data10/tantivy/dns".to_string() + &i.to_string());
        let index = TantivyService::create_index(index_path, schema.clone());
        let index_path = &("/data11/tantivy/dns".to_string() + &i.to_string());
        let index = TantivyService::create_index(index_path, schema.clone());

        let index_path = &("/data05/tantivy/tcpflow".to_string() + &i.to_string());
        let schema = TCPFlowService::get_schema();
        let index = TantivyService::create_index(index_path, schema.clone());
        let index_path = &("/data06/tantivy/tcpflow".to_string() + &i.to_string());
        let index = TantivyService::create_index(index_path, schema.clone());
        let index_path = &("/data07/tantivy/tcpflow".to_string() + &i.to_string());
        let index = TantivyService::create_index(index_path, schema.clone());
        let index_path = &("/data08/tantivy/tcpflow".to_string() + &i.to_string());
        let index = TantivyService::create_index(index_path, schema.clone());

        let index_path = &("/data02/tantivy/weblog".to_string() + &i.to_string());
        let schema = WEBLogService::get_schema();
        let index = TantivyService::create_index(index_path, schema.clone());
        let index_path = &("/data03/tantivy/weblog".to_string() + &i.to_string());
        let index = TantivyService::create_index(index_path, schema.clone());
        let index_path = &("/data04/tantivy/weblog".to_string() + &i.to_string());
        let index = TantivyService::create_index(index_path, schema.clone());
    }
}