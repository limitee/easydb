use std::thread;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

extern crate easydb;
use easydb::Column;
use easydb::Table;
use easydb::DbPool;

use std::collections::BTreeMap;

#[macro_use]
extern crate easy_util;
extern crate rustc_serialize;
use rustc_serialize::json::Json;
use rustc_serialize::json::ToJson;
use std::str::FromStr;

extern crate postgres;
use postgres::{Connection, SslMode};
use postgres::types::Type;

extern crate rand;
use rand::distributions::{IndependentSample, Range};

///convert the postgres rows to json
pub fn rows_to_json(rows:postgres::rows::Rows) -> Json {
    let mut rst_json = json!("{}");
    let mut data:Vec<Json> = Vec::new();
    for row in &rows {
        let mut back_json = json!("{}");
        let columns = row.columns();
        for column in columns {
            let name = column.name();
            let col_type = column.type_();
            match *col_type {
                Type::Int4 => {
                    let op:Option<postgres::Result<i32>> = row.get_opt(name);
                    let mut true_value:i32 = 0;
                    if let Some(rst) = op {
                        if let Ok(value) = rst {
                            true_value = value;
                        }
                    }
                    json_set!(&mut back_json; name; true_value);
                },
                Type::Int8 => {
                    let op:Option<postgres::Result<i64>> = row.get_opt(name);
                    let mut true_value:i64 = 0;
                    if let Some(rst) = op {
                        if let Ok(value) = rst {
                            true_value = value;
                        }
                    }
                    json_set!(back_json; name; true_value);
                },
                Type::Varchar | Type::Text => {
                    let op:Option<postgres::Result<String>> = row.get_opt(name);
                    let mut true_value:String = String::new();
                    if let Some(rst) = op {
                        if let Ok(value) = rst {
                            true_value = value;
                        }
                    }
                    json_set!(back_json; name; true_value);
                },
                _ => {
                    println!("ignore type:{}", col_type.name());
                },
            }
        }
        data.push(back_json);
    }
    json_set!(&mut rst_json; "data"; data);
    json_set!(&mut rst_json; "rows"; rows.len());
    rst_json
}

pub struct MyDbPool {
    dsn:String,
    conns:Vec<Mutex<Connection>>,
}

impl MyDbPool {

    pub fn new(dsn:&str, size:u32) -> MyDbPool {
        let mut conns = vec![];
        for _ in 0..size {
            let conn = match Connection::connect(dsn, SslMode::None) {
                Ok(conn) => conn,
                Err(e) => {
                    println!("Connection error: {}", e);
                    break;
                }
            };
            conns.push(Mutex::new(conn));
        }
        MyDbPool {
            dsn:dsn.to_string(),
            conns:conns,
        }
    }

    /**
     * 获得dsn字符串
     */
    pub fn get_dsn(&self) -> String {
        self.dsn.clone()
    }

    pub fn get_back_json(&self, rows:postgres::rows::Rows) -> Json {
        rows_to_json(rows)
    }

}

impl DbPool for MyDbPool {

    fn get_connection(&self) -> Result<Connection, i32> {
        let rst = match Connection::connect(self.dsn.as_str(), SslMode::None) {
            Ok(conn) => Result::Ok(conn),
            Err(e) => {
                println!("Connection error: {}", e);
                Result::Err(-1)
            }
        };
        rst
    }

    fn execute(&self, sql:&str) -> Result<Json, i32> {
        println!("{}", sql);
        let between = Range::new(0, self.conns.len());
        let mut rng = rand::thread_rng();
        let rand_int = between.ind_sample(&mut rng);
        let conn = self.conns[rand_int].lock().unwrap();

        let out_rst = {
            let rst = conn.query(sql, &[]);
            rst.and_then(|rows| {
                Result::Ok(self.get_back_json(rows))
            })
        };

        match out_rst {
            Ok(json) => {
                Result::Ok(json)
            },
            Err(err) => {
                println!("{}", err);
                Result::Err(-1)
            },
        }
    }

}

pub struct DataBase<T> {
    pub name:String,
    pub table_list:BTreeMap<String, Table<T>>,
    pub dc:Arc<T>,   //data center
}

impl<T:DbPool> DataBase<T> {

    fn get_test_table(dc:Arc<T>) -> Table<T>
    {
        let mut map = BTreeMap::new();
        let col = Column::new("id", "bigserial", -1, "not null", true);
        map.insert(col.name.clone(), col);
        let col = Column::new("name", "varchar", 40, "not null", true);
        map.insert(col.name.clone(), col);
        let pass_col = Column::new("password", "varchar", 40, "not null", false);
        map.insert(pass_col.name.clone(), pass_col);
        let nickname_col = Column::new("nickname", "varchar", 40, "not null", false);
        map.insert(nickname_col.name.clone(), nickname_col);
        let age_col = Column::new("age", "int", -1, "", false);
        map.insert(age_col.name.clone(), age_col);
        let id_col = Column::new("id", "serial", -1, "", false);
        map.insert(id_col.name.clone(), id_col);
        Table::new("test", map, dc)
    }

    fn get_blog_table(dc:Arc<T>) -> Table<T>
    {
        let mut map = BTreeMap::new();
        let id_col = Column::new("id", "serial", -1, "", false);
        map.insert(id_col.name.clone(), id_col);
        let col = Column::new("title", "varchar", 255, "not null", true);
        map.insert(col.name.clone(), col);
        let pass_col = Column::new("body", "text", -1, "", false);
        map.insert(pass_col.name.clone(), pass_col);
        Table::new("blog", map, dc)
    }

    pub fn new(name:&str, dc:Arc<T>) -> DataBase<T>
    {
        let mut table_list = BTreeMap::new();

        let test_table = DataBase::get_test_table(dc.clone());
        println!("{}", test_table.to_ddl_string());
        table_list.insert(test_table.name.clone(), test_table);

        let blog_table = DataBase::get_blog_table(dc.clone());
        println!("{}", blog_table.to_ddl_string());
        table_list.insert(blog_table.name.clone(), blog_table);

        DataBase {
            name:name.to_string(),
            table_list:table_list,
            dc:dc,
        }
    }

    pub fn get_table(&self, name:&str) -> Option<&Table<T>>
    {
        self.table_list.get(name)
    }


}

///call sql with stream result, once a row
pub fn stream<F>(conn:Connection, sql:&str, mut f:F) -> Result<i32, i32> where F:FnMut(Json) -> bool {
    let rst = conn.query("BEGIN", &[]);
    let rst = rst.or_else(|err| {
        println!("{}", err);
        Result::Err(-1)
    });
    let rows = try!(rst);
    let json = rows_to_json(rows);
    println!("{}", json);

  	let cursor_sql = format!("DECLARE myportal CURSOR FOR {}", sql);
   	println!("{}", cursor_sql);
   	let rst = conn.query(&cursor_sql, &[]);
    let rst = rst.or_else(|err| {
        println!("{}", err);
        Result::Err(-1)
    });
    let rows = try!(rst);
    let json = rows_to_json(rows);
    println!("{}", json);

    let fetch_sql = "FETCH NEXT in myportal";
    println!("{}", fetch_sql);

    loop {
        let rst = conn.query(&fetch_sql, &[]);
        let rst = rst.or_else(|err| {
            println!("{}", err);
            Result::Err(-1)
        });
        let rows = try!(rst);
        let mut json = rows_to_json(rows);
        let mut json_obj = json.as_object_mut().unwrap();
        let rows_node = json_obj.remove("rows").unwrap();
        let row_count = rows_node.as_i64().unwrap();
        if row_count < 1 {
            break;
        }
        let mut data_node = json_obj.remove("data").unwrap();
        let mut array = data_node.as_array_mut().unwrap();
        let data = array.remove(0);
        let f_back = f(data);
        if !f_back {
            break;
        }
    }

    //close the portal
   	let close_sql = "CLOSE myportal";
    println!("{}", close_sql);
    let rst = conn.query(&close_sql, &[]);
    let rst = rst.or_else(|err| {
        println!("{}", err);
        Result::Err(-1)
    });
    let rows = try!(rst);
    let json = rows_to_json(rows);
    println!("{}", json);

    //end the cursor
    let end_sql = "END";
	println!("{}", end_sql);
	let rst = conn.query(&end_sql, &[]);
    let rst = rst.or_else(|err| {
        println!("{}", err);
        Result::Err(-1)
    });
    let rows = try!(rst);
    let json = rows_to_json(rows);
    println!("{}", json);

    Result::Ok(1)
}

fn main() {
    let _ = try_main();
}

fn try_main() -> Result<i32, i32>
{
    let dsn = "postgresql://postgres:1988lm@localhost/test";
    let my_dc:MyDbPool = MyDbPool::new(dsn, 10);

    let conn = my_dc.get_connection()?;
    let _ = stream(conn, "select * from test where name='a'", |set| {
        println!("{}", set);
        true
    });

    let conn = my_dc.get_connection()?;
    let _ = stream(conn, "select * from test where name='bbc'", |set| {
        println!("{}", set);
        true
    });

    let my_db = DataBase::new("main", Arc::new(my_dc));
    let table = my_db.get_table("test").expect("table not exists.");
    let cdata = Json::from_str("{\"$or\":[{\"id\":1},{\"name\":\"123\"}],\"id\":2}").unwrap();
    let doc = Json::from_str("{}").unwrap();
    let op = Json::from_str("{}").unwrap();
    let rst = table.find_one(&cdata, &doc, &op);

    Ok(1)
}
