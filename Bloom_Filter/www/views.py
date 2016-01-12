from django.shortcuts import render
from django.template import RequestContext, loader
# Create your views here.
def index(request):
	response = ""
	doc_type = ""
	country = ""
	doc_id = ""
	if request.method == 'POST':
		doc_type = request.POST['doc_type']
		country = request.POST['country']
		doc_id = request.POST['doc_id']
	response = bloom(doc_type,doc_id,country)
	context = {'resultat' : response}
	return render(request, 'www/index.html',context)

def search(request):
	return index(request)

def bloom(doc_type,doc_id,country):
	return True