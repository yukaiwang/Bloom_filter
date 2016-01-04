#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'NHS'

import mmh3
import bitarray
import math


class BloomFilter:

    def __init__(self, nb_elem, false_positive_proba):
        # Nombre d'éléments
        self.nb_elem = nb_elem
        # Probabilité du faux-positif
        self.false_positive_proba = false_positive_proba
        # La taille du tableau de bits
        self.size = int(self.getSize())
        # Nombre de la fonction Hash
        self.nb_hash = self.getNbHash()
        # Créer un tableau de bits dont la taille est size
        self.bitArray = bitarray.bitarray(self.size)
        self.bitArray.setall(0)

    # Calculer la taille du tableau de bits en fonction
    # du nombre d'élément et la probabilité des faux-positifs
    def getSize(self):
        size = -self.nb_elem * math.log(self.false_positive_proba) / math.pow(math.log(2),2)
        return size

    # Calculer le nombre de la fonction de Hash (les positions de bits pour hasher)
    def getNbHash(self):
        nb_hash = math.log(2)*self.size/self.nb_elem
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
    return mmh3.hash(key,seed)


if __name__ == '__main__':
    bloom = BloomFilter(100000000,0.1)
    print bloom.size
    print bloom.nb_hash
