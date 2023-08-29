-- 数据导入校验表
CREATE TABLE IF NOT EXISTS check_import_record(
    `id` UInt32 , -- 逻辑自增id
    `ctime` DateTime DEFAULT now(), -- 创建时间
    `kind` String, -- 自增关联操作
    `val` String, -- 关联操作对应值
    `from_count` UInt32, -- 源文件记录数
    `in_count` UInt32, -- 导入记录数
    `suss` UInt8 DEFAULT 0 -- 0待比对 1 成功导入，2导入失败
)ENGINE = ReplacingMergeTree
ORDER BY id;