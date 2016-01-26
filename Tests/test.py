import csv
import random
import sys
import string
import time
from cassandra.cluster import Cluster

# with open('/Users/yukaiwang/Documents/INSA/5IF/PDC03/CountryCode.csv', 'rU') as f:
# 	reader = csv.reader(f, dialect=csv.excel_tab)
# 	country_list = []
# 	for row in reader:
# 		country_list.append(row[0])

# print len(country_list)
# 		num_passport = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(9))
# with open('Type.csv', 'w') as fp:
#   a = csv.writer(fp)
#   i = 0
#   data = []
#   while i < 100:
# 		type_document = ''.join(random.choice(string.ascii_uppercase) for _ in range(3))
# 		temp = []
# 		temp.append(type_document)
# 		data.append(temp)
# 		i += 1
#   a.writerows(data)

# with open('TestFile10000.csv', 'r') as f:
# 	reader = csv.reader(f)
# 	documents = []
# 	for row in reader:
# 		key = row[0] + row[1] + row[2]
# 		print key
# 		documents.append(key)
# 	print len(documents)


cluster = Cluster()
session = cluster.connect('pdc03')

prepared_stmt = session.prepare ( "SELECT count(*) FROM documents WHERE (type = ?) AND (country = ?) AND (number = ?);")
bound_stmt = prepared_stmt.bind(['PVY', 'AFG', '7PUEMHNNUAFG0177990'])
results = session.execute(bound_stmt)
print results
for result in results:
	print result.count
document = 'PVYAFG7PUEMHNNUAFG0177990'
type_doc = document[:3]
country = document[3:6]
num_doc = document[6:]
print type_doc
print country
print num_doc