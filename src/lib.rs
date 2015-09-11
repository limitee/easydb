use std::collections::BTreeMap;

extern crate rustc_serialize;
use rustc_serialize::json::Json;

mod util;
use util::DbUtil;

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
    pub fn get_options(&self, options:Json) -> String {
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

}

