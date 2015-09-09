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

/**
 * 数据库的表
 */
pub struct Table {
    pub name:String,    //表名
    pub col_list:BTreeMap<String, Column>,
}


