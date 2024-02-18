import re

def two_sentences_match(a, b):
	a = re.sub('[^A-Za-z0-9]+', '', a.lower().replace(" ","thisisaspace")).replace("thisisaspace"," ").replace("  "," ").split(" ")
	b = re.sub('[^A-Za-z0-9]+', '', b.lower().replace(" ","thisisaspace")).replace("thisisaspace"," ").replace("  "," ").split(" ")

	for i in [a,b]:
		for j in i:
			if j[-1:] == 's':
				i[i.index(j)] = j[:-1]
	ratio = len(set(a).intersection(b)) / max(min(len(a), len(b)),3)
	return ratio


def get_results_title_details(title, data_combined):
	print('get_results_title_details')
	try:
		results_title = []
		for i in data_combined:
			score = two_sentences_match(title, i['title'])
			print(score)
			if score > 0.7 and i['title'] not in [d['title'] for d in results_title if 'title' in d]:
				if 'images' in list(i.keys()) and len(i["images"]) > 0:
					img = i["images"]
					try:
						results_title.append({"title": i["title"], "url": i["url"], "description": i["description"],
											  "recalldate": i["recalldate"], "hazard": i['hazards'], "image": img})
					except IndexError:
						results_title.append({"title": i["title"], "url": i["url"], "description": i["description"],
											  "recalldate": i["recalldate"], "image": img})

				else:
					try:
						results_title.append({"title": i["title"], "url": i["url"], "description": i["description"],
											  "recalldate": i["recalldate"], "hazard": i['hazards']})
					except IndexError:
						results_title.append({"title": i["title"], "url": i["url"], "description": i["description"],
											  "recalldate": i["recalldate"]})
		return {
			'success': True,
			'count_title': len(results_title),
			'results_title': results_title
		}
	except ValueError as e:
		return {'error': str(e)}

#dummy data coming from
#https://www.amazon.com/Babycook-Stainless-Reservoir-Capacity-Midnight/dp/B07RL8HP3W/ref=sr_1_3?dchild=1&keywords=Babycook+Neo+steam+cooker%2Fblenders&qid=1591899212&sr=8-3

data_combined = [{'title': "Beaba Recalls Baby Food Steam Cooker/Blenders Due to Laceration Hazard"
, 'recalldate':'May 15, 2019'
, 'url':'url'
, 'description':'description'
, 'hazard':'hazard'
, 'image':'image'},]
title = "BEABA Babycook Neo, Glass Baby Food Maker, Glass 4 in 1 Steam Cooker & Blender, Comes with Stainless Steel Basket and Reservoir, Cook at Home, 5.5 Cup Capacity (Midnight)"

#print result of API
print(get_results_title_details(title, data_combined))


#other recalls
#https://www.amazon.com/Little-Experimenter-Globe-Kids-Built/dp/B07Q4FHN32/ref=sr_1_2?dchild=1&keywords=Little+Experimenter+3-in-1+Globes&qid=1591894293&sr=8-2
#https://www.amazon.com/Stokke-Portable-Bouncer-Multiple-Positions/dp/B08174D2X2/ref=sr_1_3?dchild=1&keywords=Stokke+Steps+Bouncers&qid=1591896270&sr=8-3
#https://www.amazon.com/Disney-Pixar-Forky-Plush-Story/dp/B07YVKXQWX/ref=sr_1_1?dchild=1&keywords=Disney+Forky+11”+Plush+Toys&qid=1591896442&sr=8-1