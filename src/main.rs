extern crate easydb;
use easydb::Column;
use easydb::Table;
use easydb::DbCenter;

use std::collections::BTreeMap;

extern crate rustc_serialize;
use rustc_serialize::json::Json;


struct MyDbCenter {
    name:String,    
}

impl DbCenter for MyDbCenter {

    fn execute(&self, sql:&str) -> Json {
        println!("{}", sql);
        Json::from_str("{\"$set\":{\"name\":\"123\"}, \"$inc\":{\"age\":10}}").unwrap()    
    }

}

fn main()
{

    let my_dc:MyDbCenter = MyDbCenter {
        name:"test".to_string(), 
    };
	let col = Column {
		name:"name".to_string(),
		ctype:"varchar".to_string(),
		length:40,
		desc:"not null".to_string(),
        escape:true,
	};
	println!("the column's name is {}.", col.name);
	println!("the kv pair is {}.", col.get_kv_pair("=", 10));

	let mut map = BTreeMap::new();
	map.insert(col.name.clone(), col);

	let pass_col = Column::new("password", "varchar", 40, "not null", false);
	println!("the ddl col string is {}.", pass_col.to_ddl_string());
	map.insert(pass_col.name.clone(), pass_col);

    let nickname_col = Column::new("nickname", "varchar", 40, "not null", false);
    println!("the ddl col string is {}.", nickname_col.to_ddl_string());
    map.insert(nickname_col.name.clone(), nickname_col);

    let age_col = Column::new("age", "int", -1, "", false);
    println!("the ddl col string is {}.", age_col.to_ddl_string());
    map.insert(age_col.name.clone(), age_col);


	let table = Table {
		name:"test".to_string(),
		col_list:map,
        dc:&my_dc,
	};
	println!("the table's name is {}.", table.name);
	println!("the table's column count is {}.", table.col_list.len());

	println!("{}", table.to_ddl_string());

	let data = Json::from_str("{\"sort\": [{\"name\":1}, {\"id\":-1}], \"limit\": 1, \"offset\": 10, \"ret\":{\"id\":1}}").unwrap();
	let op = table.get_options(&data);
	println!("the op is {}.", op);

	let cdata = Json::from_str("{\"age\":{\"$in\":[1, 2, 3]}, \"nickname\":{\"$lt\":\"abc\"}, \"name\":\"lim'ing\", \"$or\":[{\"name\":\"liming\"}, {\"password\":\"123\"}]}").unwrap();
	println!("the condition is {}", table.condition(&cdata, "name"));

    let up_data = Json::from_str("{\"$set\":{\"name\":\"123\"}, \"$inc\":{\"age\":10}}").unwrap();
    table.get_update_str(&up_data);
    
    let count_data = Json::from_str("{}").unwrap();
    let count_options = Json::from_str("{}").unwrap();
    table.count(&count_data, &count_options);
    
    let fd_cond = Json::from_str("{}").unwrap();
    let fd_data = Json::from_str("{}").unwrap();
    let fd_options = Json::from_str("{}").unwrap();
    let fd_back = table.find(&fd_cond, &fd_data, &fd_options); 
    println!("the back is {}", fd_back);
}
