#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'NHS'

import mmh3
import bitarray
import math
import csv
import os
from DBCassandra import DBCassandra


class BloomFilter(object):

    def __init__(self, nb_elem, false_positive_proba, file_object=None):
        # Nombre d'éléments
        self.nb_elem = nb_elem
        # Probabilité du faux-positif
        self.false_positive_proba = false_positive_proba
        # La taille du tableau de bits
        self.size = self.getSize() + 8 - (self.getSize() % 8)
        # Nombre de la fonction Hash
        self.nb_hash = self.getNbHash()
        if file_object is not None:
            self.bitArray = bitarray.bitarray()
            self.bitArray.fromfile(file_object)
        else:
            # Créer un tableau de bits dont la taille est size
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
                print "Ce document n'existe pas"
                return 0
        print "Ce document peut exister"
        return 1


def hash_func(key, seed):
    hash_key = mmh3.hash(key,seed)
    return hash_key


def insertData():
    db = DBCassandra(['127.0.0.1'],9042)
    db.connect('pdc03')
    with open('../donnees/temp.csv', 'r') as csvfile:
        reader = csv.reader(csvfile.read().splitlines())
        for row in reader:
            row_str = "".join(row)
            print row_str
            CQLString = "INSERT INTO documents (type, country, number) VALUES ('"+row[0]+"', '"+row[1]+"', '"+row[2]+"');"
            db.session.execute(CQLString)
    db.close()

def write_bloom_filter():
    bloom = BloomFilter(100000000,0.1,None)
    with open('../donnees/temp.csv', 'r') as csvfile:
        reader = csv.reader(csvfile.read().splitlines())
        for row in reader:
            row_str = "".join(row)
            print row_str
            bloom.add(row_str)
            bloom.exist(row_str)
    with open("bloom","wb+") as bloom_file:
        print bloom.getSize()
        print len(bloom.bitArray)
        bloom.bitArray.tofile(bloom_file)

def read_bloom_filter():
    with open("bloom","rb") as bloom_file:
        bloom = BloomFilter(100000000, 0.1, bloom_file)
        return bloom

if __name__ == '__main__':
    # insertData()
    # write_bloom_filter()
    bloom = read_bloom_filter()
    print len(bloom.bitArray)
    # with open('../donnees/temp.csv', 'r') as csvfile:
    #     reader = csv.reader(csvfile.read().splitlines())
    #     for row in reader:
    #         row_str = "".join(row)
    #         print row_str
    #         bloom.exist(row_str)
    type_doc = "TES"
    country = "JPN"
    number = "K100000000JPN1111111"
    key = type_doc + country + number
    print key
    if bloom.exist(key):
        db = DBCassandra(['127.0.0.1'],9042)
        db.connect('pdc03')
        CQLString = "SELECT * FROM documents WHERE type = '"+type_doc+"' and country = '"+country+"' and number = '"+number+"';"
        print CQLString
        results = db.session.execute(CQLString)
        for result in results:
            print result.type+result.country+result.number
        db.close()
    else:
        print ("Doc n'existe pas")


