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
        let mut rst_json = json!("{}");
        let mut data:Vec<Json> = Vec::new();
        for row in &rows {
            let mut back_json = json!("{}");
            let columns = row.columns();
            for column in columns {
                let name = column.name();
                match *column.type_() {
                    Type::Int4 => {
                        let value:i32 = row.get(name);
                        json_set!(&mut back_json; name; value);
                    },
                    Type::Int8 => {
                        let value:i64 = row.get(name);
                        json_set!(&mut back_json; name; value);
                    },
                    Type::Varchar => {
                        let value:String = row.get(name);
                        json_set!(&mut back_json; name; value);
                    },
                    Type::Text => {
                        let value:String = row.get(name);
                        json_set!(&mut back_json; name; value);
                    },
                    _ => {
                        println!("ignore type:{}", column.type_().name());
                    },
                }
            }
            data.push(back_json);
        }
        json_set!(&mut rst_json; "data"; data);
        json_set!(&mut rst_json; "rows"; rows.len());
        rst_json
    }

    fn stream<F>(&self, sql:&str, mut f:F) -> Result<i32, i32> where F:FnMut(Json) -> bool + 'static {
        let conn = try!(self.get_connection());
        let rst = conn.query("BEGIN", &[]);

        //begin
        let rst = rst.and_then(|rows| {
            let json = self.get_back_json(rows);
            println!("{}", json);
            Result::Ok(1)
        }).or_else(|err|{
            println!("{}", err);
            Result::Err(-1)
        });

        //cursor
        let cursor_sql = format!("DECLARE myportal CURSOR FOR {}", sql);
        println!("{}", cursor_sql);
        let rst = conn.query(&cursor_sql, &[]);
        let rst = rst.and_then(|rows|{
            let json = self.get_back_json(rows);
            println!("{}", json);
            Result::Ok(1)
        }).or_else(|err|{
            println!("{}", err);
            Result::Err(-1)
        });

        let rst = rst.and_then(|_| {
            let fetch_sql = "FETCH NEXT in myportal";
            println!("{}", fetch_sql);

            let mut flag = 0;
            loop {
                let rst = conn.query(&fetch_sql, &[]);
                let _ = rst.and_then(|rows|{
                    let json = self.get_back_json(rows);
                    let rows = json_i64!(&json; "rows");
                    if rows < 1 {
                        flag = -2;
                    } else {
                        let f_back = f(json);
                        if !f_back {
                            flag = -2;
                        }
                    }
                    Result::Ok(flag)
                }).or_else(|err|{
                    println!("{}", err);
                    flag = -1;
                    Result::Err(flag)
                });
                if flag < 0 {
                    break;
                }
            }
            match flag {
                -1 => {
                    Result::Err(-1)
                },
                _ => {
                    Result::Ok(1)
                },
            }
        });

        //close the portal
        let close_sql = "CLOSE myportal";
        println!("{}", close_sql);
        let rst = conn.query(&close_sql, &[]);
        let rst = rst.and_then(|rows|{
            let json = self.get_back_json(rows);
            println!("{}", json);
            Result::Ok(1)
        }).or_else(|err|{
            println!("{}", err);
            Result::Err(-1)
        });

        //end the cursor
        let end_sql = "END";
        println!("{}", end_sql);
        let rst = conn.query(&end_sql, &[]);
        let rst = rst.and_then(|rows|{
            let json = self.get_back_json(rows);
            println!("{}", json);
            Result::Ok(1)
        }).or_else(|err|{
            println!("{}", err);
            Result::Err(-1)
        });

        rst
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


fn main()
{
    let dsn = "postgresql://postgres:1988lm@localhost/test";
    let my_dc:MyDbPool = MyDbPool::new(dsn, 10);

    let _ = my_dc.stream("select * from test where name='a'", |set| {
        println!("{}", set);
        true
    });

    let _ = my_dc.stream("select * from test where name='bbc'", |set| {
        println!("{}", set);
        true
    });

    /*
    let my_db = DataBase::new("main", Arc::new(my_dc));
    let test_table = my_db.get_table("test").expect("table not exists.");
    let fd_back = test_table.find_by_str("{}", "{}", "{}");
    println!("{}", fd_back.unwrap());


	let data = Json::from_str("{\"sort\": [{\"name\":1}, {\"id\":-1}], \"limit\": 1, \"offset\": 10, \"ret\":{\"id\":1}}").unwrap();
	let op = table.get_options(&data);
	println!("the op is {}.", op);

	let cdata = Json::from_str("{\"age\":{\"$in\":[1, 2, 3]}, \"nickname\":{\"$lt\":\"abc\"}, \"name\":\"lim'ing\", \"$or\":[{\"name\":\"liming\"}, {\"password\":\"123\"}]}").unwrap();
	println!("the condition is {}", table.condition(&cdata, "name"));

    let up_data = Json::from_str("{\"$set\":{\"name\":\"123\"}, \"$inc\":{\"age\":10}}").unwrap();
    table.get_update_str(&up_data);
    
    let count_data = Json::from_str("{}").unwrap();
    let count_options = Json::from_str("{}").unwrap();
    let count_back = table.count(&count_data, &count_options);
    println!("the count back is {}.", count_back);
    
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
    */
}
