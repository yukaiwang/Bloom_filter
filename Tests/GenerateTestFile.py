#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'yukaiwang'

from GenerateDatabase import read_country_list, read_type_list
from cassandra.cluster import Cluster
import sys
import csv
import random
import string

# def get_country_distribution():
# 	with open('CountryDistribution.csv', 'r') as f:
# 		reader = csv.reader(f)
# 		distribution = []
# 		for row in reader:
# 			distribution.append(int(row[0]))
# 	return distribution 
	

def generate_test_file(nb_documents):
	# Lire les listes de pays et de type
	country_list = read_country_list()
	country_list_size = len(country_list)
	type_list = read_type_list()
	type_list_size = len(type_list)
	documents = []
	i = 0

	# Inserer dans la base de donnees des documents
	while i < nb_documents:
		num_passport = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(9))
		country = country_list[random.randint(0, country_list_size-1)]
		type_doc = type_list[random.randint(0, type_list_size-1)]
		birthday = ''.join(random.choice(string.digits) for _ in range(6))
		identifier = '1'
		num_document = num_passport + country + birthday + identifier

		type_doc = type_list[random.randint(0, type_list_size-1)]
		documents.append([type_doc, country, num_document])
		i += 1

	cluster = Cluster()
	session = cluster.connect('pdc03')

	prepared_stmt = session.prepare ( "SELECT * FROM documents LIMIT ?")
	bound_stmt = prepared_stmt.bind([int(nb_documents*0.0001)])
	stmt = session.execute(bound_stmt)
	for row in stmt:
		documents.append([row.type, row.country, row.number])

	print len(documents)
	print documents[0:5]

	filename = 'TestFile' + str(nb_documents) + '.csv'
	with open(filename, 'w') as fp:
		a = csv.writer(fp, delimiter=',')
		a.writerows(documents)

if __name__ == '__main__':
	if len(sys.argv) > 1:
		nb_documents = int(sys.argv[1])
	else:
		nb_documents = 10000

	generate_test_file(nb_documents)