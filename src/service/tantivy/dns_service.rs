use tantivy::schema::{Schema, TEXT, STORED, TextOptions, TextFieldIndexing, STRING, IntOptions};
use tantivy::{Index, Document};
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
use crate::service::redis_service::RedisService;
use crate::service::tantivy::tantivy_service::TantivyService;
use std::thread;

pub struct DNSService {}

impl DNSService {
    pub fn get_schema() -> Schema{
        let mut schema_builder = Schema::builder();

        let access_time = schema_builder.add_text_field("access_time", STRING|STORED);
        let reply_code = schema_builder.add_u64_field("reply_code",
                                                      IntOptions::default().set_indexed().set_stored());
        let host_md5 = schema_builder.add_text_field("host_md5", STRING|STORED);
        let sport = schema_builder.add_u64_field("sport",
                                                      IntOptions::default().set_indexed().set_stored());
        let count = schema_builder.add_text_field("count", STRING|STORED);
        let sip = schema_builder.add_text_field("sip", STRING|STORED);
        let host = schema_builder.add_text_field("host", TEXT|STORED);
        let host_raw = schema_builder.add_text_field("host_raw", STRING|STORED);
        let serial_num = schema_builder.add_text_field("serial_num", STRING|STORED);
        let dns_type = schema_builder.add_u64_field("dns_type",
                                                 IntOptions::default().set_indexed().set_stored());
        let dport = schema_builder.add_u64_field("dport",
                                                    IntOptions::default().set_indexed().set_stored());
        let dip = schema_builder.add_text_field("dip", STRING|STORED);

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
        let access_time = schema.get_field("access_time").unwrap();
        let reply_code = schema.get_field("reply_code").unwrap();
        let host_md5 = schema.get_field("host_md5").unwrap();
        let sport = schema.get_field("sport").unwrap();
        let count = schema.get_field("count").unwrap();
        let sip = schema.get_field("sip").unwrap();
        let host = schema.get_field("host").unwrap();
        let host_raw = schema.get_field("host_raw").unwrap();
        let serial_num = schema.get_field("serial_num").unwrap();
        let dns_type = schema.get_field("dns_type").unwrap();
        let dport = schema.get_field("dport").unwrap();
        let dip = schema.get_field("dip").unwrap();
        let mut commit:bool = false;
        loop{
            let datas = RedisService::get_data("logcenter:skyeye-dns", batch_size, redis_client.clone()).unwrap();
            let len = datas.clone().len();
            let start = time::now();
            for data in datas{
                let mut doc = Document::new();
                let json_value = json::parse(&data).unwrap();

                if json_value["access_time"].to_string() != "null"{
                    doc.add_text(access_time, &json_value["access_time"].to_string());
                }
                if json_value["reply_code"].to_string() != "null"{
                    doc.add_u64(reply_code, json_value["reply_code"].as_u64().unwrap());
                }
                if json_value["host_md5"].to_string() != "null"{
                    doc.add_text(host_md5, &json_value["host_md5"].to_string());
                }
                if json_value["sport"].to_string() != "null"{
                    doc.add_u64(sport, json_value["sport"].as_u64().unwrap());
                }
                if json_value["count"].to_string() != "null"{
                    doc.add_text(count, &json_value["count"].to_string());
                }
                if json_value["sip"].to_string() != "null"{
                    doc.add_text(sip, &json_value["sip"].to_string());
                }
                if json_value["host"].to_string() != "null"{
                    doc.add_text(host, &json_value["host"].to_string());
                }
                if json_value["serial_num"].to_string() != "null"{
                    doc.add_text(serial_num, &json_value["serial_num"].to_string());
                }
                if json_value["dns_type"].to_string() != "null"{
                    doc.add_u64(dns_type, json_value["dns_type"].as_u64().unwrap());
                }
                if json_value["dport"].to_string() != "null"{
                    doc.add_u64(dport, json_value["dport"].as_u64().unwrap());
                }
                if json_value["dip"].to_string() != "null"{
                    doc.add_text(dip, &json_value["dip"].to_string());
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

    pub fn bulkd(index:Index, schema:Schema){
        let mut index_writer = index.writer(100_000_000).unwrap();

        let access_time = schema.get_field("access_time").unwrap();
        let reply_code = schema.get_field("reply_code").unwrap();
        let host_md5 = schema.get_field("host_md5").unwrap();
        let sport = schema.get_field("sport").unwrap();
        let count = schema.get_field("count").unwrap();
        let sip = schema.get_field("sip").unwrap();
        let host = schema.get_field("host").unwrap();
        let host_raw = schema.get_field("host_raw").unwrap();
        let serial_num = schema.get_field("serial_num").unwrap();
        let dns_type = schema.get_field("dns_type").unwrap();
        let dport = schema.get_field("dport").unwrap();
        let dip = schema.get_field("dip").unwrap();

        for i in 1..1000{
            let mut doc = Document::new();

            doc.add_text(access_time, "2019-08-01 16:28:26.508");
            doc.add_u64(reply_code, 0);
            doc.add_text(host_md5, "7cb7ba399a499134e212eebb65ef0612");
            doc.add_u64(sport, 29615);
            doc.add_text(count, "1;8;6;1");
            doc.add_text(sip, "10.95.134.108");
            doc.add_text(host, "aa.com");
            doc.add_text(serial_num, "QbJK/NOX+");
            doc.add_u64(dns_type, 1);
            doc.add_u64(dport, 63712);
            doc.add_text(dip, "220.181.38.148");

            index_writer.add_document(doc);
        }
    }
}