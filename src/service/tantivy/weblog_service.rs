use tantivy::schema::{Schema, TEXT, STORED, TextOptions, TextFieldIndexing, STRING, IntOptions};
use tantivy::{Index, Document, SegmentComponent};
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
use crate::service::redis_service::RedisService;
use crate::service::tantivy::tantivy_service::TantivyService;
use std::thread;
use std::io::Write;

pub struct WEBLogService {}

impl WEBLogService {
    pub fn get_schema() -> Schema{
        let mut schema_builder = Schema::builder();

        let origin = schema_builder.add_text_field("origin", TEXT|STORED);
        let sip = schema_builder.add_text_field("sip", STRING|STORED);
        let uri_md5 = schema_builder.add_text_field("uri_md5", STRING|STORED);
        let access_time = schema_builder.add_text_field("access_time", STRING|STORED);
        let uri = schema_builder.add_text_field("uri", TEXT|STORED);
        let agent = schema_builder.add_text_field("agent", TEXT|STORED);
        let serial_num = schema_builder.add_text_field("serial_num", STRING|STORED);
        let host = schema_builder.add_text_field("host", TEXT|STORED);
        let host_md5 = schema_builder.add_text_field("host_md5", STRING|STORED);
        let cookie = schema_builder.add_text_field("cookie", TEXT|STORED);
        let data = schema_builder.add_text_field("data", TEXT|STORED);
        let dport = schema_builder.add_u64_field("dport",
                                                 IntOptions::default().set_indexed().set_stored());
        let referer = schema_builder.add_text_field("referer", TEXT|STORED);
        let sport = schema_builder.add_u64_field("sport",
                                                 IntOptions::default().set_indexed().set_stored());
        let dip = schema_builder.add_text_field("dip", STRING|STORED);
        let method = schema_builder.add_text_field("method", STRING|STORED);
        let xff = schema_builder.add_text_field("xff", STRING|STORED);

        let schema = schema_builder.build();

        schema
    }

    pub fn bulk(index : Index, schema : Schema, commit_interval : u32, buffer : usize, batch_size : u32){
        let init_total = TantivyService::get_total(index.clone(), schema.clone());
        let init_start = time::now();
        let thread_name = thread::current().name().unwrap().to_string();
        println!("thread: {:?}, start at: {:?}, total: {:?}", thread_name, init_start, init_total);
        let redis_client = redis::Client::open("redis://10.95.134.109:6379").unwrap();
        let mut i = 1;
        let mut index_writer = index.writer(buffer).unwrap();
        
        let origin = schema.get_field("origin").unwrap();
        let sip = schema.get_field("sip").unwrap();
        let uri_md5 = schema.get_field("uri_md5").unwrap();
        let access_time = schema.get_field("access_time").unwrap();
        let uri = schema.get_field("uri").unwrap();
        let agent = schema.get_field("agent").unwrap();
        let serial_num = schema.get_field("serial_num").unwrap();
        let host = schema.get_field("host").unwrap();
        let host_md5 = schema.get_field("host_md5").unwrap();
        let cookie = schema.get_field("cookie").unwrap();
        let data_f = schema.get_field("data").unwrap();
        let dport = schema.get_field("dport").unwrap();
        let referer = schema.get_field("referer").unwrap();
        let sport = schema.get_field("sport").unwrap();
        let dip = schema.get_field("dip").unwrap();
        let method = schema.get_field("method").unwrap();
        let xff = schema.get_field("xff").unwrap();

        let mut commit:bool = false;
        loop{
            let datas = RedisService::get_data("logcenter:skyeye-weblog", batch_size, redis_client.clone()).unwrap();
            let len = datas.clone().len();
            let start = time::now();
            for data in datas{
                let mut doc = Document::new();
                let json_value = json::parse(&data).unwrap();

                if json_value["origin"].to_string() != "null"{
                    doc.add_text(origin, &json_value["origin"].to_string());
                }
                if json_value["sip"].to_string() != "null"{
                    doc.add_text(sip, &json_value["sip"].to_string());
                }
                if json_value["uri_md5"].to_string() != "null"{
                    doc.add_text(uri_md5, &json_value["uri_md5"].to_string());
                }
                if json_value["access_time"].to_string() != "null"{
                    doc.add_text(access_time, &json_value["access_time"].to_string());
                }
                if json_value["uri"].to_string() != "null"{
                    doc.add_text(uri, &json_value["uri"].to_string());
                }
                if json_value["agent"].to_string() != "null"{
                    doc.add_text(agent, &json_value["agent"].to_string());
                }
                if json_value["serial_num"].to_string() != "null"{
                    doc.add_text(serial_num, &json_value["serial_num"].to_string());
                }
                if json_value["host"].to_string() != "null"{
                    doc.add_text(host, &json_value["host"].to_string());
                }
                if json_value["host_md5"].to_string() != "null"{
                    doc.add_text(host_md5, &json_value["host_md5"].to_string());
                }
                if json_value["cookie"].to_string() != "null"{
                    doc.add_text(cookie, &json_value["cookie"].to_string());
                }
                if json_value["data"].to_string() != "null"{
                    doc.add_text(data_f, &json_value["data"].to_string());
                }
                if json_value["dport"].to_string() != "null"{
                    doc.add_u64(dport, json_value["dport"].as_u64().unwrap());
                }
                if json_value["referer"].to_string() != "null"{
                    doc.add_text(referer, &json_value["referer"].to_string());
                }
                if json_value["sport"].to_string() != "null"{
                    doc.add_u64(sport, json_value["sport"].as_u64().unwrap());
                }
                if json_value["dip"].to_string() != "null"{
                    doc.add_text(dip, &json_value["dip"].to_string());
                }
                if json_value["method"].to_string() != "null"{
                    doc.add_text(method, &json_value["method"].to_string());
                }
                if json_value["xff"].to_string() != "null"{
                    doc.add_text(xff, &json_value["xff"].to_string());
                }

                index_writer.add_document(doc);
            }
            if i % commit_interval == 0{
                index_writer.commit();
                let end = time::now();
                let total = TantivyService::get_total(index.clone(), schema.clone());
                println!("thread: {:?}, total docs: {:?}, total added: {:?}, bulk size: {:?}, took: {:?}, total took: {:?}"
                         ,thread_name, total, total - init_total, len, end - start, end - init_start);
            }else{
                let end = time::now();
                println!("thread: {:?}, bulk size: {:?}, took: {:?}", thread_name, len, end - start);
            }
            i = i + 1;
        }
    }
}