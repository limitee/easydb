extern crate easydb;
use easydb::Column;

fn main()
{
	let col = Column {
		name:"name".to_string(),
		ctype:"varchar".to_string(),
		length:40,
		desc:"user's name".to_string(),
	};
	println!("the column's name is {}.", col.name);
}
