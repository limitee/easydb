use std::collections::BTreeMap;

extern crate rustc_serialize;
use rustc_serialize::json::Json;

extern crate regex;
use regex::Regex;

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
}

/**
 * 数据库的一列
 */
pub struct Column {
    pub name:String,    //名称
    pub ctype:String,    //类型
    pub length:i32,     //长度
    pub desc:String,    //其他信息
}

impl Column {

    pub fn new(name:&str, ctype: &str, length:i32, desc: &str) -> Column {
        Column {
            name: name.to_string(),
            ctype: ctype.to_string(),
            length: length,
            desc: desc.to_string(),
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
        if self.ctype == "integer" || self.ctype == "bigint" {
            exp = exp + &value.to_string();
        }
        else if op == "in" {
            exp = exp + &value.to_string();
        }
        else {
            //TODO escape
            exp = exp + "'" + &value.to_string() + "'";
        }
        exp
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
pub struct Table {
    pub name:String,    //表名
    pub col_list:BTreeMap<String, Column>,
}

impl Table {

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
            ret = ret + " limit " + &x.as_i64().unwrap().to_string();
        };
        //offset属性是一个整数
        if let Some(x) = options_obj.get("offset") {
            ret = ret + " offset " + &x.as_i64().unwrap().to_string();
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
                    let mut op:&str = "";
                    if key == "gt" {
                        op = ">";
                    }
                    else if key == "gte" {
                        op = ">=";
                    }
                    else if key == "lt" {
                        op = "<";
                    }
                    else if key == "lte" {
                        op = "<=";
                    }
                    exp = exp + &parent_col.get_kv_pair(op, DbUtil::get_pure_json_string(&value)); 
                }
                ret = ret + &exp + ")";
            }
            else { //未匹配上，值是object，递归调用condition，否则，组成kv字符串
                let mut kv:String = "(".to_string();
                if value.is_object() {  //值是一个对象,递归调用condition方法
                    self.condition(&value, key);         
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

}

