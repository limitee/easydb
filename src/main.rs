extern crate easydb;
use easydb::Column;

fn main()
{
	let col = Column {
		name:"name".to_string(),
	};
	println!("the column's name is {}.", col.name);
}
