import BaseConfig

import json
from urllib import request
import pymysql
import time

'''
	基于高德地图实现省市区数据初始化
'''

dbConn = pymysql.connect(host='localhost',
						 port=3306,
						 user='root',
						 password='root',
						 database='python_db')

# 获取环境变量中配置的高德KEY
GD_KEY = BaseConfig.os.environ["GD_KEY"]
if GD_KEY is None:
	raise BaseConfig.BizException("GD_KEY is not set")

GD_URL = "https://restapi.amap.com/v3/config/district"

# 调用高德查询
'''
	subdistrict：子级行政区
	规则：设置显示下级行政区级数（行政区级别包括：国家、省/直辖市、市、区/县、乡镇/街道多级数据）
	可选值：0、1、2、3等数字，并以此类推
	0：不返回下级行政区；
	1：返回下一级行政区；
	2：返回下两级行政区；
	3：返回下三级行政区；
'''
def requestGD(name, subdistrict):
	print(name.decode('utf-8'))
	time.sleep(1)

	#拉取高德地图省市区数据
	key = "?key=" + request.quote(GD_KEY) + "&keywords=" + request.quote(name) + "&subdistrict=" + request.quote(str(subdistrict))
	url_all = GD_URL + key
	req = request.Request(url_all)
	districts_str = request.urlopen(req).read()

	# 反序列化
	districts = json.loads(districts_str)

	# 检查请求是否成果
	if districts['status'] == '0' :
		raise BaseConfig.BizException(districts['info'])

	data = districts['districts']
	return data;

def initDBTable():
	createTableSqlArr = ["DROP TABLE IF EXISTS `districts`;", \
			'''CREATE TABLE IF NOT EXISTS `districts` (
  			  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  			  `parent_adcode` varchar(12) NOT NULL DEFAULT '' COMMENT '上级区域编码',
  		      `citycode` varchar(12) NOT NULL DEFAULT '' COMMENT '城市编码',
  			  `adcode` varchar(12) NOT NULL DEFAULT '' COMMENT '区域编码',
			  `name` varchar(64) NOT NULL DEFAULT '' COMMENT '行政区名称',
			  `level` varchar(12) NOT NULL DEFAULT '' COMMENT '行政区划级别',
			  `center` varchar(24) NOT NULL DEFAULT '' COMMENT '区域中心点',
			  `weight` int(11) NOT NULL DEFAULT '0' COMMENT '行政区权重',
			  `is_delete` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除：0-未删除；1-已删除',
			  `gmt_create` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `gmt_modify` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
			  PRIMARY KEY (`id`),
			  UNIQUE KEY `uniq_adcode_X_citycode` (`adcode`,`citycode`),
			  KEY `idx_name` (`name`),
			  KEY `idx_level` (`level`),
			  KEY `idx_parent_adcode` (`parent_adcode`)
			) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COMMENT='行政区列表';''']

	try:
		with dbConn.cursor() as cursor:
			for sql in createTableSqlArr:
				cursor.execute(sql)
			print("数据库表初始化完成")
	except Exception as e:
		print("数据库表初始化失败:", e)
		raise BaseConfig.BizException(e)

def insertDB(data, parentAdcode, weight):
	# 判断是否存在城市编码
	citycodeLength = 0
	for item in data:
		citycodeLength += len(item["citycode"])

	# 构造插入语句，当存在城市编码或当前为街道级别数据，增加城市编码插入
	sql = "insert into districts(parent_adcode, adcode, name, level, center, weight) values (%s, %s, %s, %s, %s, %s)"
	if citycodeLength > 0 or data[0]["level"] == 'street':
		sql = "insert into districts(parent_adcode, citycode, adcode, name, level, center, weight) values (%s, %s, %s, %s, %s, %s, %s)"

	dbDataList = []
	index = 1
	for item in data:
		if citycodeLength > 0:
			if data[0]["level"] == 'street':
				# 街道城市编码基于返回值顺序+1
				dbDataList.append((parentAdcode, item['citycode'] + "-" + str(index),
								   item['adcode'], item['name'], item['level'], item['center'], weight))
				index += 1
			else:
				if len(item['citycode']) > 0:
					dbDataList.append((parentAdcode, item['citycode'], item['adcode'], item['name'],
								   item['level'], item['center'], weight))
				else:
					dbDataList.append((parentAdcode, "", item['adcode'], item['name'],
									   item['level'], item['center'], weight))
		else:
			if data[0]["level"] == 'street':
				# 对于街道城市编码为空的数据，基于上级城市编码+1生成
				dbDataList.append((parentAdcode, parentAdcode + "-" + str(index), item['adcode'], item['name'],
								   item['level'], item['center'], weight))
				index += 1
			else:
				dbDataList.append((parentAdcode, item['adcode'], item['name'], item['level'], item['center'], weight))

	try:
		with dbConn.cursor() as cursor:
			# 执行插入语句
			cursor.executemany(sql, dbDataList)

			# 提交事务
			dbConn.commit()
	except Exception as e:
		# 发生错误时回滚
		dbConn.rollback()
		print("数据插入失败:", e)
		print(dbDataList)

def initDistrict(countryName):
	# 插入根节点
	data = requestGD(countryName, 0)
	insertDB(data, '0', 1)

	# 获取省份数据
	districts = requestGD(countryName, 1)

	#省份权重
	weight = 1
	# 遍历查询省份明细
	for district in districts[0]['districts']:
		name = district['name'].encode("utf-8")
		data = requestGD(name, 3)
		recursionInsert(data, "100000", weight)
		weight += 1

def recursionInsert(data, parentAdcode, weight):
	insertDB(data, parentAdcode, weight)
	for	item in data:
		if len(item["districts"]) > 0:
			recursionInsert(item["districts"], item["adcode"], weight)


def main():
	try:
		# 初始化数据库省市区表接口
		initDBTable()
		# 初始化省市区数据
		initDistrict("中国".encode("utf-8"))
	except BaseConfig.BizException as e:
		print("业务异常：", e)
	finally:
		if dbConn is not None:
			dbConn.close()
#主函数
main()



