use tantivy::schema::Schema;
use tantivy::Index;
use tantivy::query::QueryParser;
use tantivy::collector::TopDocs;

pub struct TantivyService{}

impl TantivyService{
    pub fn create_index(index_path : &str, schema : Schema) -> Index{
        let index = Index::create_in_dir(index_path, schema.clone()).unwrap();
        index
    }

    pub fn open_index(index_path:&str) -> Index{
        let index = Index::open_in_dir(index_path).unwrap();
        index
    }

    pub fn search(index : Index, schema : Schema){
        let reader = index.reader().unwrap();

        let searcher = reader.searcher();
        let host = schema.get_field("host").unwrap();
        let dport = schema.get_field("dport").unwrap();

        let query_parser = QueryParser::for_index(&index, vec![host]);

        let query = query_parser.parse_query("zu7sp").unwrap();
        let total = searcher.num_docs();
        println!("{:?}", total);
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();

        for(_score, doc_address) in top_docs{
            let retrieved_doc = searcher.doc(doc_address).unwrap();
            println!("{}", schema.to_json(&retrieved_doc));
        }
    }

    pub fn get_total(index : Index, schema : Schema) -> u64{
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let total = searcher.num_docs();
        total
    }
}
