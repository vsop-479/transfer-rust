extern crate reqwest;
use std::io::Read;
use self::reqwest::Client;

pub struct ToshiService{}

impl ToshiService{
    pub fn put_data(index_name : &str, data : String, client : &Client){
        println!("put data: {}", data);
//    只能String + &str + &str
        let mut url = "http://localhost:8080/".to_string() + index_name + "/_bulk";
        let mut response = client.post(&url).body(data)
            .send().expect("Failed to send request");
        let mut buf = String::new();
        response.read_to_string(&mut buf).expect("Failed to read response");
        println!("put data result: {}", buf);
    }

    fn get_order(){
        let mut response = reqwest::get("http://localhost:8080/order").unwrap();
        let mut buf = String::new();
        response.read_to_string(&mut buf).expect("Failed to read response");
        println!("{}", buf);
    }

    fn get_order2(){
        let mut response = reqwest::get("http://localhost:8080/order")
            .expect("Failed to send request");
        println!("{}", response.status());
        let mut buf = String::new();
        response.read_to_string(&mut buf).expect("Failed to read response");
        println!("{}", buf);
    }

    fn bulk(){
        let client = reqwest::Client::new();
        let mut body = r#"
        {"order_id": 1008, "user_id": 25, "user_name": "dabao", "addr": "beijing fengtai shilihe", "total_price": 100}
{"order_id": 1009, "user_id": 25, "user_name": "dabao", "addr": "beijing fengtai shilihe", "total_price": 100}
{"order_id": 1010, "user_id": 25, "user_name": "dabao", "addr": "beijing fengtai shilihe", "total_price": 100}
{"order_id": 1011, "user_id": 25, "user_name": "dabao", "addr": "beijing fengtai shilihe", "total_price": 100}
{"order_id": 1012, "user_id": 25, "user_name": "dabao", "addr": "beijing fengtai shilihe", "total_price": 100}
{"order_id": 1008, "user_id": 25, "user_name": "dabao", "addr": "beijing fengtai shilihe", "total_price": 100}
{"order_id": 1009, "user_id": 25, "user_name": "dabao", "addr": "beijing fengtai shilihe", "total_price": 100}
{"order_id": 1010, "user_id": 25, "user_name": "dabao", "addr": "beijing fengtai shilihe", "total_price": 100}
{"order_id": 1011, "user_id": 25, "user_name": "dabao", "addr": "beijing fengtai shilihe", "total_price": 100}
{"order_id": 1012, "user_id": 25, "user_name": "dabao", "addr": "beijing fengtai shilihe", "total_price": 100}"#;

        let mut response = client.post("http://localhost:8080/order/_bulk").body(body)
            .send().expect("Failed to send request");
        let mut buf = String::new();
        response.read_to_string(&mut buf).expect("Failed to read response");
        println!("{}", buf);
    }

    fn query_order(){
        let client = reqwest::Client::new();
        let mut body = r#"{ "query": {"term": {"addr": "beijing" } }, "limit": 10 }"#;
        let mut response = client.post("http://localhost:8080/order").body(body)
            .send().expect("Failed to send request");

        let mut buf = String::new();
        response.read_to_string(&mut buf).expect("Failed to read response");
        println!("{}", buf);
    }

    fn summary_order(){
        let client = reqwest::Client::new();
        let mut response = client.get("http://localhost:8080/order/_summary")
            .send().expect("Failed to send request");

        let mut buf = String::new();
        response.read_to_string(&mut buf).expect("Failed to read response");
        println!("{}", buf);
    }
}
