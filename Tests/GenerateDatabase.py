#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'yukaiwang'

from cassandra.cluster import Cluster
import sys
import csv
import random
import string

def read_country_list():
	with open('CountryCode.csv', 'rU') as f:
		reader = csv.reader(f)
		country_list = []
		for row in reader:
			country_list.append(row[0])
	return country_list

def read_type_list():
	with open('Type.csv', 'rU') as f:
		reader = csv.reader(f)
		type_list = []
		for row in reader:
			type_list.append(row[0])
	return type_list

def generate_database():
	cluster = Cluster()
	session = cluster.connect('pdc03')
  
  # Vider la table
	session.execute("TRUNCATE pdc03.documents;")

  # Creer 100.000 de documents dans la base de donnees
	if len(sys.argv) > 1:
		nb_documents = int(sys.argv[1])
	else:
		nb_documents = 100000

	# Lire les listes de pays et de type
	country_list = read_country_list()
	country_list_size = len(country_list)
	type_list = read_type_list()
	type_list_size = len(type_list)
	i = 0

	# Inserer dans la base de donnees des documents
	while i < nb_documents:
		num_passport = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(9))
		country = country_list[random.randint(0, country_list_size-1)]
		type_doc = type_list[random.randint(0, type_list_size-1)]
		birthday = ''.join(random.choice(string.digits) for _ in range(6))
		num_document = num_passport + country + birthday

		prepared_stmt = session.prepare ( "INSERT INTO documents (type, country, number) VALUES (?, ?, ?)")
		bound_stmt = prepared_stmt.bind([type_doc, country, num_document])
		stmt = session.execute(bound_stmt)
		i += 1

if __name__ == '__main__':
	generate_database()
