#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'NHS'

import mmh3
import bitarray
import math
import csv
import os
from cassandra.cluster import Cluster

class BloomFilter(object):

	def __init__(self, nb_elem, false_positive_proba):
		# Nombre d'éléments
		self.nb_elem = nb_elem
		# Probabilité du faux-positif
		self.false_positive_proba = false_positive_proba
		# La taille du tableau de bits
		self.size = self.getSize() + 8 - (self.getSize() % 8)
		# Nombre de la fonction Hash
		self.nb_hash = self.getNbHash()
		# Initialiser le bit array
		self.bitArray = bitarray.bitarray(self.size)
		self.bitArray.setall(0)

	# Calculer la taille du tableau de bits en fonction
	# du nombre d'élément et la probabilité des faux-positifs
	def getSize(self):
		size = int(-self.nb_elem * math.log(self.false_positive_proba) / math.pow(math.log(2),2))
		return size

	# Calculer le nombre de la fonction de Hash (les positions de bits pour hasher)
	def getNbHash(self):
		nb_hash = int(math.log(2)*self.size/self.nb_elem)
		return nb_hash

	def add(self, key):
		for seed in xrange(self.nb_hash):
			bit_pos = hash_func(key, seed) % self.size
			self.bitArray[bit_pos] = 1

	def exist(self, key):
		for seed in xrange(self.nb_hash):
			bit_pos = hash_func(key,seed) % self.size
			if self.bitArray[bit_pos] == 0:
				# print "Ce document n'existe pas, il est valid !"
				return 0
		# print "Ce document peut exister, il est peut-être non valid..."
		return 1

def hash_func(key, seed):
	hash_key = mmh3.hash(key,seed)
	return hash_key