def generate_test_file():
	country_distribution = get_country_distribution()
	country_list = read_country_list()
	country_list_size = len(country_list)
	random.shuffle(country_list)
	type_list = read_type_list()
	type_list_size = len(type_list)
	
	documents = []
	i = 0
	for country in country_list:
		for j in range(country_distribution[i]):
			num_passport = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(9))
			birthday = ''.join(random.choice(string.digits) for _ in range(6))
			identifier = '1'
			num_document = num_passport + country + birthday + identifier

			type_doc = type_list[random.randint(0, type_list_size-1)]
			documents.append([type_doc, country, num_document])
		i += 1

	documents.append(['PVY', 'ABW', '0VV6GN14YABW6257590'])
	print len(documents)
	print documents[0:5]

	with open('TestFile10000.csv', 'w') as fp:
		a = csv.writer(fp, delimiter=',')
		a.writerows(documents)
