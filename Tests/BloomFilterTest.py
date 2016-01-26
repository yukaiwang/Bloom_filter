# -*- coding: utf-8 -*-
__author__ = 'yukaiwang'

import mmh3
import bitarray
import math
import csv
import os
import sys
import time
from BloomFilter import BloomFilter
from cassandra.cluster import Cluster

def set_bitarray(bloom_filter, session):
	results = session.execute("SELECT * FROM documents")
	for row in results:
		document = row.type + row.country + row.number
		bloom_filter.add(document)

def read_test_file(filename):
	documents = []
	with open(filename, 'r') as f:
		reader = csv.reader(f)
		for row in reader:
			document = row[0] + row[1] + row[2]
			documents.append(document)
	return documents

# Les requêtes passent uniquement dans le filtre de bloom
def test_with_bloom_fiter(bloom_filter, documents):
	start_time = time.time()
	nb = 0
	for document in documents:
		if bloom_filter.exist(document) == 1:
			# Incrémenter le nombre de document qui existe dans la base de données
			nb += 1
	return (nb, time.time() - start_time)

# Les requêtes passent uniquement dans la base de données
def test_with_database(session, documents):
	start_time = time.time()
	nb = 0
	for document in documents:
		type_doc = document[:3]
		country = document[3:6]
		num_doc = document[6:]
		prepared_stmt = session.prepare ( "SELECT count(*) FROM documents WHERE (type = ?) AND (country = ?) AND (number = ?);")
		bound_stmt = prepared_stmt.bind([type_doc, country, num_doc])
		results = session.execute(bound_stmt)
		for result in results:
			if result.count != 0:
				nb += 1
	return (nb, time.time() - start_time)

# Les requêtes passent par le filtre de bloom et puis passent éventuellement dans la base de données
def test_with_both(session, bloom_filter, documents):
	start_time = time.time()
	nb = 0
	for document in documents:
		if bloom_filter.exist(document) == 1:
			type_doc = document[:3]
			country = document[3:6]
			num_doc = document[6:]
			prepared_stmt = session.prepare ( "SELECT count(*) FROM documents WHERE (type = ?) AND (country = ?) AND (number = ?);")
			bound_stmt = prepared_stmt.bind([type_doc, country, num_doc])
			results = session.execute(bound_stmt)
			for result in results:
				if result.count != 0:
					nb += 1
	return (nb, time.time() - start_time)
	
def print_result(potential_no_valid, duration, nb_requests):
	false_positive_proba = (potential_no_valid - nb_requests * 0.0001)/nb_requests
	print "Le temps d'execution: %.2f seconds" % duration
	print "La proportion de faux-positifs réelle: %g" % false_positive_proba

if __name__ == '__main__':
	if len(sys.argv) > 1:
		nb_requests = int(sys.argv[1])
		false_positive_proba = float(sys.argv[2])
	else:
		nb_requests = 10000
		false_positive_proba = 0.1

	cluster = Cluster()
	session = cluster.connect('pdc03')

	bloom_filter = BloomFilter(100000, false_positive_proba)
	set_bitarray(bloom_filter, session)
	filename = 'TestFile' + str(nb_requests) + '.csv'
	documents = read_test_file(filename)

	print 'Evaluation de la performance du filtre de bloom'
	print ''
	print 'Le nombre de requêtes à passer: %g' % nb_requests
	print 'La proportion de faux-positifs du filtre de bloom: %g' % false_positive_proba
	print ''
	# Premier test
	print '1. Les requêtes passent uniquement dans le filtre de bloom'
	potential_no_valid, duration = test_with_bloom_fiter(bloom_filter, documents)
	print_result(potential_no_valid, duration, nb_requests)
	print ''
	# Deuxieme test
	# print '2. Les requêtes passent uniquement dans la base de données'
	# potential_no_valid, duration = test_with_database(session, documents)
	# print_result(potential_no_valid, duration, nb_requests)
	# print ''
	# # Troisieme test
	# print '3. Les requêtes passent par le filtre de bloom et puis passent éventuellement dans la base de données'
	# potential_no_valid, duration = test_with_both(session, bloom_filter, documents)
	# print_result(potential_no_valid, duration, nb_requests)


