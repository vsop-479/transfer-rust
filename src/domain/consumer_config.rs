#[derive(Clone)]
pub struct ConsumerConfig {
    pub index_name : String,
    pub data_key : String,
    pub batch : u32,
    pub thread_num : u32
}

impl ConsumerConfig {
    pub fn build_consumer_config(index_name : String, data_key : String, batch : u32, thread_num : u32)
                                 -> ConsumerConfig {
        ConsumerConfig {
            index_name,
            data_key,
            batch,
            thread_num,
        }
    }
}

//impl Clone for ConsumerConfig{
//    fn clone(&self) -> Self {
//        self.index_name,
//        self.
//    }
//}