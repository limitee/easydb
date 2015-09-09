extern crate easydb;
use easydb::Column;
use easydb::Table;

use std::collections::BTreeMap;

fn main()
{
	let col = Column {
		name:"name".to_string(),
		ctype:"varchar".to_string(),
		length:40,
		desc:"user's name".to_string(),
	};
	println!("the column's name is {}.", col.name);

	let mut map = BTreeMap::new();
	map.insert(col.name.clone(), col);
	let table = Table {
		name:"test".to_string(),
		col_list:map,
	};
	println!("the table's name is {}.", table.name);
	println!("the table's column count is {}.", table.col_list.len());
}
