extern crate easydb;
use easydb::Column;
use easydb::Table;

use std::collections::BTreeMap;

extern crate rustc_serialize;
use rustc_serialize::json::Json;

fn main()
{
	let col = Column {
		name:"name".to_string(),
		ctype:"varchar".to_string(),
		length:40,
		desc:"not null".to_string(),
	};
	println!("the column's name is {}.", col.name);

	let mut map = BTreeMap::new();
	map.insert(col.name.clone(), col);

	let pass_col = Column::new("password", "varchar", 40, "not null");
	println!("the ddl col string is {}.", pass_col.to_ddl_string());
	map.insert(pass_col.name.clone(), pass_col);

	let table = Table {
		name:"test".to_string(),
		col_list:map,
	};
	println!("the table's name is {}.", table.name);
	println!("the table's column count is {}.", table.col_list.len());

	println!("{}", table.to_ddl_string());

	let data = Json::from_str("{\"sort\": {\"id\":-1, \"name\":1}, \"bar\": \"baz\"}").unwrap();
	let op = table.get_options(data);
	println!("the op is {}.", op);
}
