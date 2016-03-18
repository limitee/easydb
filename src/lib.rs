use std::collections::BTreeMap;

#[macro_use]
extern crate easy_util;
extern crate rustc_serialize;
use rustc_serialize::json::Json;
use rustc_serialize::json::ToJson;
use std::str::FromStr;

extern crate regex;
use regex::Regex;

use std::rc::{Rc};
use std::sync::Arc;

extern crate postgres;
use postgres::{Connection};

#[macro_use]
extern crate log;

pub trait DbPool {
    fn execute(&self, sql:&str) -> Result<Json, i32>;

    /**
     * 获得一个新的连接
     */
    fn get_connection(&self) -> Result<Connection, i32>;

    fn stream<F>(&self, sql:&str, mut f:F) -> Result<i32, i32> where F:FnMut(Json) -> bool + 'static;
}


/**
 * 数据库辅助工具类
 */
pub struct DbUtil;

impl DbUtil {
    
    /**
     * 获得json基础类型的字符串表达形式
     */
    pub fn get_pure_json_string(data:&Json) -> String {
        let ret = match *data {
            Json::String(ref x) => format!("{}", x),
            _ => data.to_string(),
        };
        ret
    }
   
    /**
     * 获得sql所需的字符串表达形式
     */
    pub fn get_sql_string(data:&Json) -> String {
        let ret = match *data {
            Json::String(ref x) => format!("'{}'", x),
            _ => data.to_string(),
        };
        ret
    }

    /**
     * escape字符串
     */
    pub fn escape(value:&str) -> String {
        let mut escaped:String = String::new();
        escaped.push('\'');
        let mut hasBackslash = false;
        for c in value.chars() {
            if c == '\'' {
                escaped.push(c);
                escaped.push(c);
            }
            else if c == '\\' {
                escaped.push(c);
                escaped.push(c);
                hasBackslash = true;
            }
            else {
                escaped.push(c);
            }
        }
        escaped.push('\'');

        if hasBackslash {
            format!(" E{}", escaped)
        }
        else {
            escaped
        }
    }

}

/**
 * 数据库的一列
 */
pub struct Column {
    pub name:String,    //名称
    pub ctype:String,    //类型
    pub length:i32,     //长度
    pub desc:String,    //其他信息
    pub escape:bool,
}

impl Column {

    pub fn new(name:&str, ctype: &str, length:i32, desc: &str, escape:bool) -> Column {
        Column {
            name: name.to_string(),
            ctype: ctype.to_string(),
            length: length,
            desc: desc.to_string(),
            escape:escape,
        }
    }

    /**
     * 获得列的ddl字符串
     */
    pub fn to_ddl_string(&self) -> String {
        let mut str:String = format!("{} {}", self.name, self.ctype);
        if self.length > 0 {
            str = str + "(" + &self.length.to_string() + ")";
        }
        str = str + " " + &self.desc;
        str
    }

    /**
     * 把键值对转换成sql表达式
     */
    pub fn get_kv_pair<T:ToString>(&self, op:&str, value:T) -> String
    {
        let mut exp:String = self.name.clone() + " " + op + " ";
        if self.ctype == "integer" || self.ctype == "bigint" || self.ctype == "serial" || self.ctype == "bigserial" {
            exp = exp + &value.to_string();
        }
        else if op == "in" {
            exp = exp + &value.to_string();
        }
        else {
            if self.escape {
                exp = exp + &DbUtil::escape(&value.to_string());
            }
            else {
                exp = exp + "'" + &value.to_string() + "'";
            }
        }
        exp
    }

    /**
     * 获得column对应的值
     */
    pub fn get_value(&self, value:&Json) -> String {
        let mut value_str = String::new();
        if self.ctype == "int" || self.ctype == "integer" || self.ctype == "bigint" {
            value_str = value.to_string(); 
        }
        else {
            if self.escape {
                value_str = DbUtil::escape(&DbUtil::get_pure_json_string(value));
            }
            else {
                value_str = "'".to_string() + &DbUtil::get_pure_json_string(value) + "'";
            }
        }
        value_str
    }

    /**
     * 获得列的名称
     */
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    /**
     * 获得列的类型
     */
    pub fn get_ctype(&self) -> String {
        self.ctype.clone()
    }

}

/**
 * 数据库的表
 */
pub struct Table<T> {
    pub name:String,    //表名
    pub col_list:BTreeMap<String, Column>,  //列的列表
    pub dc:Arc<T>,   //data center
}

impl<T:DbPool> Table<T> {

    pub fn new(name:&str, col_list:BTreeMap<String, Column>, dc:Arc<T>) -> Table<T>
    {
        Table {
            name:name.to_string(),
            col_list:col_list,
            dc:dc,
        }
    }

    /**
     * 获得表的ddl语句
     */
    pub fn to_ddl_string(&self) -> String {
        let mut str:String = format!("create table {} (", self.name);
        let mut count = 0;
        for (_, value) in self.col_list.iter() {
            if count > 0 {
                str = str + ",";
            }
            str = str + &value.to_ddl_string();
            count = count + 1;
        }
        str = str + ");";
        str
    }

    /**
     * 获得附加参数的字符串表达形式
     */
    pub fn get_options(&self, options:&Json) -> String {
        let mut ret:String = "".to_string();
        let options_obj = options.as_object().unwrap();
        //sort属性必须对应一个数组，如[{\"a\":1, \"b\":-1}]
        if let Some(x) = options_obj.get("sort") {
            let sort_obj = x.as_array().unwrap();
            let length = sort_obj.len();
            for x in 0..length {
                let sort_obj_tmp = sort_obj[x].as_object().unwrap();
                if x > 0 {
                    ret = ret + ", ";
                }
                else
                {
                    ret = ret + " order by ";
                }
                for (key, value) in sort_obj_tmp.iter() {
                    ret = ret + key;
                    if value.as_i64().unwrap() > 0 {
                        ret = ret + " asc";
                    }
                    else {
                        ret = ret + " desc";
                    }
                }
            }
        };
        //limit属性是一个整数
        if let Some(x) = options_obj.get("limit") {
            let limit = x.as_i64().unwrap();
            if limit > 0
            {
                ret = format!("{} limit {}", ret, limit);
            }
        };
        //offset属性是一个整数
        if let Some(x) = options_obj.get("offset") {
            let offset = x.as_i64().unwrap();
            if offset > 0
            {
                ret = format!("{} offset {}", ret, offset);
            }
        };
        //ret定义更新时要返回的数据
        if let Some(x) = options_obj.get("ret") {
            let ret_obj = x.as_object().unwrap();
            let mut count = 0;
            for (key, _) in ret_obj.iter() {
                if count > 0 {
                    ret = ret + ", ";
                }
                else
                {
                    ret = ret + " returning ";
                }
                ret = ret + key;
                count = count + 1;
            }
        };
        ret
    }


    /**
     * 获得更新的sql
     */
    pub fn get_update_str(&self, data:&Json) -> String {
        let mut ret:String = "".to_string();
        let re = Regex::new(r"\$([a-z]+)").unwrap();
        let data_obj = data.as_object().unwrap();
        let mut set_count:i32 = 0;
        for (key, value) in data_obj.iter() {
            let iter = re.captures_iter(key);
            if let Some(x) = iter.last() {
                let key:&str = x.at(1).unwrap_or("");
                if key == "set" {
                    let set_obj = (&value).as_object().unwrap();
                    for (set_key, set_value) in set_obj.iter() {
                        if set_count > 0 {
                            ret = ret + ",";
                        }
                        let col_option:Option<&Column> = self.col_list.get(set_key);
                        if col_option.is_some() {
                            let col = col_option.unwrap();
                            ret = ret + &col.get_kv_pair("=", DbUtil::get_pure_json_string(&set_value));
                            set_count = set_count + 1;
                        }
                    }
                }
                else if key == "inc" {
                    let inc_obj = (&value).as_object().unwrap();
                    for (inc_key, inc_value) in inc_obj.iter() {
                        if set_count > 0 {
                            ret = ret + ",";
                        }
                        let col_option:Option<&Column> = self.col_list.get(inc_key);
                        if col_option.is_some() {
                            ret = ret + inc_key + " = " + inc_key + " + " + &inc_value.to_string();
                            set_count = set_count + 1;
                        }
                    }
                }
            }
        }
        //println!("the ret is {}.", ret);
        ret
    }

    /**
     * 获得更新的sql
     */
    pub fn get_upsert_str(&self, data:&Json) -> String {
        let mut ret:String = "".to_string();
        let re = Regex::new(r"\$([a-z]+)").unwrap();
        let data_obj = data.as_object().unwrap();
        let mut set_count:i32 = 0;
        for (key, value) in data_obj.iter() {
            let iter = re.captures_iter(key);
            if let Some(x) = iter.last() {
                let key:&str = x.at(1).unwrap_or("");
                if key == "set" {
                    let set_obj = (&value).as_object().unwrap();
                    for (set_key, set_value) in set_obj.iter() {
                        if set_count > 0 {
                            ret = ret + ",";
                        }
                        let col_option:Option<&Column> = self.col_list.get(set_key);
                        if col_option.is_some() {
                            let col = col_option.unwrap();
                            ret = ret + &col.get_kv_pair("=", DbUtil::get_pure_json_string(&set_value));
                            set_count = set_count + 1;
                        }
                    }
                }
                else if key == "inc" {
                    let inc_obj = (&value).as_object().unwrap();
                    for (inc_key, inc_value) in inc_obj.iter() {
                        if set_count > 0 {
                            ret = ret + ",";
                        }
                        let col_option:Option<&Column> = self.col_list.get(inc_key);
                        if col_option.is_some() {
                            ret = ret + inc_key + " = " + &self.name + "." + inc_key + " + " + &inc_value.to_string();
                            set_count = set_count + 1;
                        }
                    }
                }
            }
        }
        //println!("the ret is {}.", ret);
        ret
    }

    /**
     * 获得data条件所表示的sql的where条件字符串
     */
    pub fn condition(&self, data:&Json, parent_col_name:&str) -> String {
        let parent_col_option:Option<&Column> = self.col_list.get(parent_col_name);
        let mut ret:String = "".to_string();
        let mut count:u32 = 0;

        let re = Regex::new(r"\$([a-z]+)").unwrap();
        
        let data_obj = data.as_object().unwrap();
        for (key, value) in data_obj.iter() {
            if count > 0 {
                ret = ret + " and ";
            }
            let iter = re.captures_iter(key);
            //如果匹配上 
            if let Some(x) = iter.last() {
                let mut exp:String = "(".to_string();
                let key:&str = x.at(1).unwrap_or("");
                if parent_col_option.is_some() {
                    let parent_col:&Column = parent_col_option.unwrap();
                    if key == "gt" {
                        exp = exp + &parent_col.get_kv_pair(">", DbUtil::get_pure_json_string(&value)); 
                    }
                    else if key == "gte" {
                        exp = exp + &parent_col.get_kv_pair(">=", DbUtil::get_pure_json_string(&value)); 
                    }
                    else if key == "lt" {
                        exp = exp + &parent_col.get_kv_pair("<", DbUtil::get_pure_json_string(&value)); 
                    }
                    else if key == "lte" {
                        exp = exp + &parent_col.get_kv_pair("<=", DbUtil::get_pure_json_string(&value)); 
                    }
                    else if key == "ne" {
                        exp = exp + &parent_col.get_kv_pair("!=", DbUtil::get_pure_json_string(&value)); 
                    }
                    else if key == "or" {
                        let or_data_array:&Vec<Json> = value.as_array().unwrap(); 
                        let mut or_count:i32 = 0;
                        for or_json in or_data_array {
                            if or_count > 0 {
                                exp = exp + " or ";
                            }
                            exp = exp + &self.condition(or_json, "");    
                            or_count = or_count + 1;
                        }
                    }
                    else if key == "in" {
                        let in_data_array:&Vec<Json> = value.as_array().unwrap();
                        let mut in_count:i32 = 0;
                        let mut in_string:String = "".to_string();
                        for in_json in in_data_array {
                            if in_count > 0 {
                                in_string = in_string + ",";
                            }
                            in_string = in_string + &DbUtil::get_sql_string(in_json);
                            in_count = in_count + 1;
                        }
                        exp = exp + &parent_col.get_kv_pair("in", in_string);
                    }
                }
                ret = ret + &exp + ")";
            }
            else { //未匹配上，值是object，递归调用condition，否则，组成kv字符串
                let mut kv:String = "(".to_string();
                if value.is_object() {  //值是一个对象,递归调用condition方法
                    kv = kv + &self.condition(&value, key);         
                } else {
                    if let Some(x) = self.col_list.get(key) {
                        kv = kv + &x.get_kv_pair("=", DbUtil::get_pure_json_string(&value));
                    }
                }
                ret = ret + &kv + ")";
            };

            count = count + 1;
        }
        //println!("the parent col is {}.", parent_col.to_ddl_string());
        ret
    }

    /**
     * get count by the condition. 
     */
    pub fn count(&self, data:&Json, options:&Json) -> Result<Json, i32> {
        let mut sql:String = "select count(*) from ".to_string() + &self.name;
        let cond = self.condition(data, "");
        if cond.len() > 0 {
            sql = sql + " where " + &cond;
        }
        sql = sql + &self.get_options(options);
        self.dc.execute(&sql)
    }

    /**
     * get count by the condition str.
     */
    pub fn count_by_str(&self, data:&str, options:&str) -> Result<Json, i32> {
        let c_data = Json::from_str(data).unwrap();
        let c_options = Json::from_str(options).unwrap();
        self.count(&c_data, &c_options)
    }
    
    /**
     * sql的select语句
     *
     */
    pub fn find(&self, cond:&Json, data:&Json, options:&Json) -> Result<Json, i32> {
        let mut sql:String = "select ".to_string();
        let mut key_str:String = String::new();
        let columns = data.as_object().unwrap();
        let mut col_count:i32 = 0;
        for (key, value) in columns.iter() {
            if col_count > 0 {
                key_str = key_str + ",";
            }
            key_str = key_str + key;
            col_count = col_count + 1;
        }
        if key_str.len() == 0 {
            key_str = "*".to_string();
        }
        sql = sql + &key_str + " from " + &self.name;
        let cond:String = self.condition(cond, "");
        if cond.len() > 0 {
            sql = sql + " where " +  &cond;
        }
        sql = sql + &self.get_options(options);
        self.dc.execute(&sql)
    }

    pub fn find_one(&self, cond:&Json, data:&Json, options:&Json) -> Result<Json, i32> {
        let rst = try!(self.find(cond, data, options));
        let rows = json_i64!(&rst; "rows");
        if rows > 0 {
            let ref_data = json_path!(&rst; "data", "0");
            return Result::Ok(ref_data.clone());
        } else {
            return Result::Err(-1);
        }
    }

    pub fn find_one_by_str(&self, cond:&str, data:&str, options:&str) -> Result<Json, i32> {
        let fd_cond = Json::from_str(cond).unwrap();
        let fd_data = Json::from_str(data).unwrap();
        let fd_options = Json::from_str(options).unwrap();
        self.find_one(&fd_cond, &fd_data, &fd_options)
    }

    /**
     * sql的select语句
     */
    pub fn find_by_str(&self, cond:&str, data:&str, options:&str) -> Result<Json, i32> {
        let fd_cond = Json::from_str(cond).unwrap();
        let fd_data = Json::from_str(data).unwrap();
        let fd_options = Json::from_str(options).unwrap();
        self.find(&fd_cond, &fd_data, &fd_options)
    }
    
    pub fn save_by_str(&self, data:&str, options:&str) -> Result<Json, i32> {
        let j_data = Json::from_str(data).unwrap();
        let j_op = Json::from_str(options).unwrap();
        self.save(&j_data, &j_op)
    }

    pub fn update_by_str(&self, cond:&str, data:&str, options:&str) -> Result<Json, i32> {
        let p_cond = Json::from_str(cond).unwrap();
        let p_data = Json::from_str(data).unwrap();
        let p_op = Json::from_str(options).unwrap();
        self.update(&p_cond, &p_data, &p_op)
    }

    pub fn update(&self, cond:&Json, data:&Json, options:&Json) -> Result<Json, i32> {
        let mut sql:String = "update ".to_string() + &self.name + " set ";
        sql = sql + &(self.get_update_str(data));
        let cond:String = self.condition(cond, "");
        if cond.len() > 0 {
            sql = sql + " where " + &cond;
        }
        sql = sql + &self.get_options(options);
        self.dc.execute(&sql)
    }

    pub fn upsert(&self, conflict:&Json, data:&Json, up_data:&Json, options:&Json) -> Result<Json, i32> {
        let mut sql:String = "insert into ".to_string() + &self.name + " (";
        let data_obj = data.as_object().unwrap();
        let mut data_obj_key_count:i32 = 0;
        let mut key_str:String = String::new();
        let mut value_str:String = String::new();
        for (key, value) in data_obj.iter() {
            let col_option:Option<&Column> = self.col_list.get(key);
            if col_option.is_some() {
                let col:&Column = col_option.unwrap();
                if data_obj_key_count > 0 {
                    key_str = key_str + ",";
                    value_str = value_str + ",";
                }
                key_str = key_str + key;
                value_str = value_str + &col.get_value(value);
                data_obj_key_count = data_obj_key_count + 1;
            }
        }
        sql = sql + &key_str + ") values (" + &value_str + ")" + &self.get_options(options);

        //define conflict keys
        let mut data_obj_key_count:i32 = 0;
        let mut key_str:String = String::new();
        for (key, _) in conflict.as_object().unwrap().iter() {
            let col_option:Option<&Column> = self.col_list.get(key);
            if col_option.is_some() {
                let col:&Column = col_option.unwrap();
                if data_obj_key_count > 0 {
                    key_str = key_str + ",";
                }
                key_str = key_str + key;
                data_obj_key_count = data_obj_key_count + 1;
            }
        }
        sql = sql + " ON CONFLICT (" + &key_str + ") DO UPDATE SET ";
        sql = sql + &(self.get_upsert_str(up_data));

        self.dc.execute(&sql)
    }

    /**
     * 保存数据到数据库
     */
    pub fn save(&self, data:&Json, options:&Json) -> Result<Json, i32> {
        let mut sql:String = "insert into ".to_string() + &self.name + " (";
        let data_obj = data.as_object().unwrap();
        let mut data_obj_key_count:i32 = 0;
        let mut key_str:String = String::new();
        let mut value_str:String = String::new();
        for (key, value) in data_obj.iter() {
            let col_option:Option<&Column> = self.col_list.get(key);
            if col_option.is_some() {
                let col:&Column = col_option.unwrap();
                if data_obj_key_count > 0 {
                    key_str = key_str + ",";
                    value_str = value_str + ",";
                }
                key_str = key_str + key; 
                value_str = value_str + &col.get_value(value);
                data_obj_key_count = data_obj_key_count + 1;
            }
        }
        sql = sql + &key_str + ") values (" + &value_str + ")" + &self.get_options(options);
        self.dc.execute(&sql)
    }

    /**
     * 删除符合条件的数据
     */
    pub fn remove(&self, cond:&Json, options:&Json) -> Result<Json, i32> {
        let mut sql:String = "delete from ".to_string() + &self.name;
        let cond:String = self.condition(cond, "");
        if cond.len() > 0 {
            sql = sql + " where " + &cond; 
        }
        sql = sql + &self.get_options(options);
        self.dc.execute(&sql)
    }

    /**
     * 删除符合条件的数据
     */
    pub fn remove_by_str(&self, cond:&str, options:&str) -> Result<Json, i32> {
        let p_cond = Json::from_str(cond).unwrap();
        let p_op = Json::from_str(options).unwrap();
        self.remove(&p_cond, &p_op)
    }
}










































