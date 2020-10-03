# go_tick_min

#### 介绍
tick 2 min golang版本

#### 软件架构
读取xml.tar.gz数据,逐行处理合成分钟数据并入库.


#### 安装教程
创建postgres库,创建schema: future.
创建分钟表
```sql
CREATE TABLE future.future_min (
	"DateTime" timestamp NOT NULL,
	"Instrument" varchar(32) NOT NULL,
	"Open" float4 NOT NULL,
	"High" float4 NOT NULL,
	"Low" float4 NOT NULL,
	"Close" float4 NOT NULL,
	"Volume" int4 NOT NULL,
	"OpenInterest" float8 NOT NULL,
	"TradingDay" varchar(8) NOT NULL,
	CONSTRAINT future_min_datetime_instrument PRIMARY KEY ("DateTime", "Instrument")
);
CREATE INDEX future_min_instrument_idx ON future.future_min USING btree ("Instrument");
CREATE INDEX future_min_instrument_tradingdayidx ON future.future_min USING btree ("Instrument", "TradingDay");
CREATE INDEX future_min_tradingday ON future.future_min USING btree ("TradingDay");
```

### 使用说明
#### 环境变量
* tickCsvPath tick文件路径
* pgConfig postgres配置
  `postgres://postgres:123456@172.19.129.98:25432/postgres?sslmode=disable`
#### 空库
没有数据的表可以用 run 20120814 即tick文件开始交易日执行