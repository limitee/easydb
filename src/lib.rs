use std::collections::BTreeMap;

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
    pub fn get_ddl_string(&self) -> String {
        let mut str:String = format!("create table {} (", self.name);
        str
    }

}

