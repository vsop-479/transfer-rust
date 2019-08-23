use tantivy::schema::{Schema, TEXT, STORED, TextOptions, TextFieldIndexing, STRING, IntOptions};
use tantivy::{Index, Document};
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;
use crate::service::redis_service::RedisService;
use crate::service::tantivy::tantivy_service::TantivyService;
use std::thread;

pub struct TCPFlowService{}

impl TCPFlowService {
    pub fn get_schema() -> Schema{
        let mut schema_builder = Schema::builder();

        let status = schema_builder.add_text_field("status", STRING|STORED);
        let dst_mac = schema_builder.add_text_field("dst_mac", STRING|STORED);
        let sip = schema_builder.add_text_field("sip", STRING|STORED);
        let downlink_length = schema_builder.add_u64_field("downlink_length",
                                                           IntOptions::default().set_indexed().set_stored());
        let down_payload = schema_builder.add_text_field("down_payload", TEXT|STORED);
        let proto = schema_builder.add_text_field("proto", STRING|STORED);
        let dtime = schema_builder.add_text_field("dtime", STRING|STORED);
        let client_os = schema_builder.add_text_field("client_os", TEXT|STORED);
        let up_payload = schema_builder.add_text_field("up_payload", TEXT|STORED);
        let serial_num = schema_builder.add_text_field("serial_num", STRING|STORED);
        let server_os = schema_builder.add_text_field("server_os", TEXT|STORED);
        let summary = schema_builder.add_text_field("summary", TEXT|STORED);
        let stime = schema_builder.add_text_field("stime", STRING|STORED);
        let uplink_length = schema_builder.add_u64_field("uplink_length",
                                                         IntOptions::default().set_indexed().set_stored());
        let dport = schema_builder.add_u64_field("dport",
                                                 IntOptions::default().set_indexed().set_stored());
        let sport = schema_builder.add_u64_field("sport",
                                                 IntOptions::default().set_indexed().set_stored());
        let dip = schema_builder.add_text_field("dip", STRING|STORED);
        let src_mac = schema_builder.add_text_field("src_mac", STRING|STORED);

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

        let status = schema.get_field("status").unwrap();
        let dst_mac = schema.get_field("dst_mac").unwrap();
        let sip = schema.get_field("sip").unwrap();
        let downlink_length = schema.get_field("downlink_length").unwrap();
        let down_payload = schema.get_field("down_payload").unwrap();
        let proto = schema.get_field("proto").unwrap();
        let dtime = schema.get_field("dtime").unwrap();
        let client_os = schema.get_field("client_os").unwrap();
        let up_payload = schema.get_field("up_payload").unwrap();
        let serial_num = schema.get_field("serial_num").unwrap();
        let server_os = schema.get_field("server_os").unwrap();
        let summary = schema.get_field("summary").unwrap();
        let stime = schema.get_field("stime").unwrap();
        let uplink_length = schema.get_field("uplink_length").unwrap();
        let dport = schema.get_field("dport").unwrap();
        let sport = schema.get_field("sport").unwrap();
        let dip = schema.get_field("dip").unwrap();
        let src_mac = schema.get_field("src_mac").unwrap();

        let mut commit:bool = false;
        loop{
            let datas = RedisService::get_data("logcenter:skyeye-tcpflow", batch_size, redis_client.clone()).unwrap();
            let len = datas.clone().len();
            let start = time::now();
            for data in datas{
                let mut doc = Document::new();
                let json_value = json::parse(&data).unwrap();

                if json_value["status"].to_string() != "null"{
                    doc.add_text(status, &json_value["status"].to_string());
                }
                if json_value["dst_mac"].to_string() != "null"{
                    doc.add_text(dst_mac, &json_value["dst_mac"].to_string());
                }
                if json_value["sip"].to_string() != "null"{
                    doc.add_text(sip, &json_value["sip"].to_string());
                }
                if json_value["downlink_length"].to_string() != "null"{
                    doc.add_u64(downlink_length, json_value["downlink_length"].as_u64().unwrap());
                }
                if json_value["down_payload"].to_string() != "null"{
                    doc.add_text(down_payload, &json_value["down_payload"].to_string());
                }
                if json_value["proto"].to_string() != "null"{
                    doc.add_text(proto, &json_value["proto"].to_string());
                }
                if json_value["dtime"].to_string() != "null"{
                    doc.add_text(dtime, &json_value["dtime"].to_string());
                }
                if json_value["client_os"].to_string() != "null"{
                    doc.add_text(client_os, &json_value["client_os"].to_string());
                }
                if json_value["up_payload"].to_string() != "null"{
                    doc.add_text(up_payload, &json_value["up_payload"].to_string());
                }
                if json_value["serial_num"].to_string() != "null"{
                    doc.add_text(serial_num, &json_value["serial_num"].to_string());
                }
                if json_value["server_os"].to_string() != "null"{
                    doc.add_text(server_os, &json_value["server_os"].to_string());
                }
                if json_value["summary"].to_string() != "null"{
                    doc.add_text(summary, &json_value["summary"].to_string());
                }
                if json_value["stime"].to_string() != "null"{
                    doc.add_text(stime, &json_value["stime"].to_string());
                }
                if json_value["uplink_length"].to_string() != "null"{
                    doc.add_u64(uplink_length, json_value["uplink_length"].as_u64().unwrap());
                }
                if json_value["dport"].to_string() != "null"{
                    doc.add_u64(dport, json_value["dport"].as_u64().unwrap());
                }
                if json_value["sport"].to_string() != "null"{
                    doc.add_u64(sport, json_value["sport"].as_u64().unwrap());
                }
                if json_value["dip"].to_string() != "null"{
                    doc.add_text(dip, &json_value["dip"].to_string());
                }
                if json_value["src_mac"].to_string() != "null"{
                    doc.add_text(src_mac, &json_value["src_mac"].to_string());
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