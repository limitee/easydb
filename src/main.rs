extern crate easydb;
use easydb::Column;
use easydb::Table;
use easydb::DbCenter;

use std::collections::BTreeMap;

extern crate rustc_serialize;
use rustc_serialize::json::Json;

extern crate postgres;
use postgres::{Connection, SslMode};

struct MyDbCenter {
    name:String,    
    conn:Connection,
}

impl DbCenter for MyDbCenter {

    fn execute(&self, sql:&str) -> Json {
        println!("{}", sql);
        let stmt = self.conn.prepare(&sql).unwrap();
        let rows = stmt.query(&[]).unwrap();
        let columns = rows.columns();
        for column in columns {
            println!("the column name is {}.", column.name());
        } 
        println!("the effected rows is {}.", rows.len());
        for row in &rows {

        }
        println!("the result is {:?}", rows);
        Json::from_str("{\"$set\":{\"name\":\"123\"}, \"$inc\":{\"age\":10}}").unwrap()    
    }

}

fn main()
{

    let dsn = "postgresql://postgres:1988lm@localhost/test";
    let conn = match Connection::connect(dsn, &SslMode::None) {
        Ok(conn) => conn,
        Err(e) => {
            println!("Connection error: {}", e);
            return;
        }
    };
    let my_dc:MyDbCenter = MyDbCenter {
        name:"test".to_string(), 
        conn:conn,
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
    
    let id_col = Column::new("id", "serial", -1, "", false);
    map.insert(id_col.name.clone(), id_col);

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
    
    let fd_cond = Json::from_str("{\"name\":\"123\"}").unwrap();
    let fd_data = Json::from_str("{}").unwrap();
    let fd_options = Json::from_str("{}").unwrap();
    let fd_back = table.find(&fd_cond, &fd_data, &fd_options); 
    println!("the back is {}", fd_back);


    let sv_data = Json::from_str("{\"name\":\"123\", \"nickname\":\"ming\", \"password\":\"123456\", \"age\":1}").unwrap();
    let sv_options = Json::from_str("{\"ret\":{\"id\":1}}").unwrap();
    let sv_back = table.save(&sv_data, &sv_options);
    println!("the save back is {}", sv_back);

    let del_cond = Json::from_str("{\"name\":\"1234\"}").unwrap();
    let del_options = Json::from_str("{}").unwrap();
    let del_back = table.remove(&del_cond, &del_options);
    println!("the del back is {}", del_back);
}
