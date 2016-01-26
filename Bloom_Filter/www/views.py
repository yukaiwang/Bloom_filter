from django.shortcuts import render
from django.template import RequestContext, loader
from BloomFilter import BloomFilter
from cassandra.cluster import Cluster

# Create your views here.
bloom_filter = None
session = None

def set_bitarray(bloom_filter, session):
	results = session.execute("SELECT * FROM documents")
	for row in results:
		document = row.type + row.country + row.number
		bloom_filter.add(document)

def initialize_filter(request):
	global bloom_filter
	global session
	cluster = Cluster()
	session = cluster.connect('pdc03')

	bloom_filter = BloomFilter(100000, 0.0001)
	set_bitarray(bloom_filter, session)

	return index(request)

def index(request):
	response = ""
	doc_type = ""
	country = ""
	doc_id = ""
	response = ''
	if request.method == 'POST':
		doc_type = request.POST['doc_type']
		country = request.POST['country']
		doc_id = request.POST['doc_id']
		print doc_id + ' ' + country + ' ' + doc_type 
		if pass_to_bloom_filter(doc_type,doc_id,country):
			response = "valid"
		else:
			response = "invalid"
	context = {'resultat' : response,'doc_type':doc_type,'country':country,'doc_id':doc_id}
	return render(request, 'www/index.html',context)

def search(request):
	return index(request)


def pass_to_bloom_filter(doc_type,doc_id,country):
	global bloom_filter
	global session
	key = doc_type + country + doc_id
	print key
	if bloom_filter.exist(key):
		prepared_stmt = session.prepare ( "SELECT count(*) FROM documents WHERE (type = ?) AND (country = ?) AND (number = ?);")
		bound_stmt = prepared_stmt.bind([doc_type, country, doc_id])
		results = session.execute(bound_stmt)
		for result in results:
			if result.count != 0:
				return False
		return True
	else:
		return True