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


fn main() {
//    create();
    let buffer = 100_000_000usize;
    let commit_interval = 10;
    let concurrency = 1;
    let batch_size = 1000;
    start(concurrency, commit_interval, buffer, batch_size);
}

fn start(concurrency : u32, commit_interval : u32, buffer : usize, batch_size : u32){
    let mut handles= Vec::new();


    handles.push(thread::Builder::new().name("weblog".to_string() + &i.to_string()).spawn(move||{
        weblog(commit_interval.clone(), buffer.clone(), batch_size.clone());
    }).unwrap());
    handles.push(thread::spawn(move||{
        tcpflow(commit_interval.clone(), buffer.clone(), batch_size.clone());
    }));
    handles.push(thread::spawn(move||{
        dns(commit_interval.clone(), buffer.clone(), batch_size.clone());
    }));

    for handle in handles {
        handle.join().unwrap();
    }
}

fn weblog(commit_interval : u32, buffer : usize, batch_size : u32){
    let index_path = "E:\\tantivy_data\\weblog";
    let schema = WEBLogService::get_schema();
    let index = TantivyService::open_index(index_path);
    WEBLogService::bulk(index, schema, commit_interval, buffer, batch_size);
}

fn tcpflow(commit_interval : u32, buffer : usize, batch_size : u32){
    let index_path = "E:\\tantivy_data\\tcpflow";
    let schema = TCPFlowService::get_schema();
    let index = TantivyService::open_index(index_path);
    TCPFlowService::bulk(index, schema, commit_interval, buffer, batch_size);
}

fn dns(commit_interval : u32, buffer : usize, batch_size : u32){
    let index_path = "E:\\tantivy_data\\dns";
    let schema = DNSService::get_schema();
    let index = TantivyService::open_index(index_path);
    DNSService::bulk(index, schema, commit_interval, buffer, batch_size);
}

fn create(){
    let index_path = "E:\\tantivy_data\\dns";
    let schema = DNSService::get_schema();
    let index = TantivyService::create_index(index_path, schema);

    let index_path = "E:\\tantivy_data\\tcpflow";
    let schema = TCPFlowService::get_schema();
    let index = TantivyService::create_index(index_path, schema);

    let index_path = "E:\\tantivy_data\\weblog";
    let schema = WEBLogService::get_schema();
    let index = TantivyService::create_index(index_path, schema);
}