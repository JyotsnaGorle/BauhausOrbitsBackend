# Check corpus json format
# http://labs.semanticscholar.org/corpus/

import os
import json
from neo4j.v1 import GraphDatabase
from neo4j.v1 import CypherError
from itertools import combinations

json_file = '../data/papers-2017-10-30-sample/papers-2017-10-30-sample.json'

# Connect to neo4j driver
driver_user = "neo4j"
driver_pass = "neo4jps"
driver = GraphDatabase.driver("bolt://localhost:7687", auth=(driver_user, driver_pass), encrypted=False)

def h_index(citations):
    """
    :type citations: List[int]
    :rtype: int
    """
    h = 0
    citations.sort(reverse=True)
    for x in citations:
        if x >= h + 1:
            h += 1
        else:
            break
    return h
    
# Add author to graph
def add_author_hindex(tx, entry):
    tx.run(
        "MATCH (a:Author {authorId: $a_id}) " +
        "SET a.hIndex = $a_hindex", a_id=entry['authorId'], a_hindex=entry['authorHIndex'])
    
    #print(".. author updated: " + str(author['authorId']))

# Add author to graph
def add_authors(tx, entry):
	
	if entry['authors'] != None:
		authors = entry['authors']

		if len(authors) > 0:
			all_author_ids = []

			for author in authors:
				author_id = 0
				author_name = ""
				
				# author ids is strangely an array, we will take the first id of the author
				if author['ids'] != None and len(author['ids']) > 0:
					author_id = int(author['ids'][0])
				if author['name'] != None:
					author_name = str(author['name'])
				
				if author_id > 0 and author_name != '':
					all_author_ids.append(author_id)

					### Relationship between author and paper: AUTHOR----AUTHOR_IN---->PAPER

					# Match for the paper (do not use MERGE here), then add the authors
					# 		Create the authors if they don't exist
					try:
						
						tx.run(
						"MATCH (p:Paper {paperId: $p_id}) " +
						"MERGE (n:Author {authorId: $a_id, name: $a_name}) " +
						"MERGE (n)-[:AUTHOR_IN]->(p)", p_id=entry['id'], a_id=author_id, a_name=author_name)
					
					except:
						pass
					

			### Relationship between author and author: AUTHOR----COLLABORATED_WITH---->AUTHOR
			# Every author collaborated with every author author in the paper
			# Combinations of 2
			if len(all_author_ids) > 0:
				all_combos = list(combinations(all_author_ids, 2))
				for combo in all_combos:
					author_1_id = combo[0]
					author_2_id = combo[1]

					try:
						tx.run(
						"MERGE (a1:Author {authorId: $a1_id}) " +
						"MERGE (a2:Author {authorId: $a2_id}) " +
						"MERGE (a1)-[:COLLABORATED_WITH]->(a2)", a1_id=author_1_id, a2_id=author_2_id)
					except:
						pass
				
			#print("-- processed authors" + "\r\n")
				
# Add paper to graph
def add_paper(tx, entry):
	
	try:
		tx.run(
			"MERGE (p:Paper {paperId: $id}) " +
			"ON CREATE SET " +
			"p.paperId = $id," +
			"p.title = $title," +
			"p.venue = $venue," +
			"p.year = $year," +
			"p.s2Url = $s2Url," +
			"p.paperAbstract = $paperAbstract," +
			"p.journalName = $journalName," +
			"p.journalPages = $journalPages," +
			"p.journalVolume = $journalVolume " +
			"ON MATCH SET " +
			"p.paperId = $id," +
			"p.title = $title," +
			"p.venue = $venue," +
			"p.year = $year," +
			"p.s2Url = $s2Url," +
			"p.paperAbstract = $paperAbstract," +
			"p.journalName = $journalName," +
			"p.journalPages = $journalPages," +
			"p.journalVolume = $journalVolume" +
			" RETURN p.paperId", id = entry['id'],title = entry['title'],venue = entry['venue'],year = entry['year'],s2Url = entry['s2Url'],paperAbstract = entry['paperAbstract'],journalName = entry['journalName'],journalPages = entry['journalPages'],journalVolume = entry['journalVolume'])
			
		# inCitations
		if len(entry['inCitations']) > 0:
			for cit in entry['inCitations']:
				tx.run(
					"MATCH (a:Paper {paperId: $a_id}) " +
					"MERGE (b:Paper {paperId: $b_id}) " +
					"MERGE (a)-[:CITED_BY]->(b)", a_id=entry['id'], b_id=cit)
				
		# outCitations
		if len(entry['outCitations']) > 0:
			for cit in entry['outCitations']:
				tx.run(
					"MATCH (a:Paper {paperId: $a_id}) " +
					"MERGE (b:Paper {paperId: $b_id}) " +
					"MERGE (b)-[:CITED_BY]->(a)", a_id=entry['id'], b_id=cit)

	except:
		pass
	
	#print("Paper: " + entry['id'])
	#print("-- processed paper")
	
#--------------------------------
#--------------------------------
#-------------MAIN--------------
#--------------------------------
#--------------------------------
with driver.session() as session:
	
	#---------------------------------------
	#------------- Constraints -------------
	#---------------------------------------
	# Paper "id" field is a unique key
	c_p = session.run("CREATE CONSTRAINT ON (paper:Paper) ASSERT paper.paperId IS UNIQUE")
	
	# Author "ids" field is a unique key
	c_a = session.run("CREATE CONSTRAINT ON (author:Author) ASSERT author.authorId IS UNIQUE")
	
	# check unique key (if success)
	#s = session.run("CALL db.constraints")
	#print(s.single()[0])
	
	with open(json_file, 'r', encoding='utf8') as data_file:
		lines = data_file.readlines()
		total_lines = len(lines)
		c = 0
		for line in lines:
			json_f = json.loads(line)
			
			# Add paper
			session.write_transaction(add_paper, json_f)
			
			# Add paper authors
			session.write_transaction(add_authors, json_f)

			print( str(c) + " / " + str(total_lines))
			c = c + 1

	data_file.close()

	########################
	# More queries on the Graph
    queries = [
    'MATCH (p:Paper) SET p.year = toInteger(p.year)',
    'MATCH (p:Paper)<-[w:AUTHOR_IN]-(a:Author) '+
	    'with a, count(p) AS totalPapers ' +
	    'SET a.totalAuthorIn = totalPapers',
    'MATCH (p:Paper)<-[w:AUTHOR_IN]-(a:Author) '+
	    'with p, count(a) AS totalAuthors ' +
	    'SET p.totalAuthors = totalAuthors',
    'MATCH (p:Paper) WHERE p.totalAuthors IS NULL SET p.totalAuthors = 0',
    'MATCH (p:Paper)-[c:CITED_BY]->() '+
	    'WITH p, count(c) AS incomingCitations ' +
	    'SET p.totalIncomingCitations = incomingCitations',
    'MATCH (p:Paper) WHERE not ((p)-[:CITED_BY]->()) SET p.totalIncomingCitations = 0',
    ]

    for query in queries:
    	x = session.run(query)
	##########################


	##########################
	# h-index
	print("--- query: Author h-index")
	results = list(session.run(
        "MATCH (p:Paper)<-[w:AUTHOR_IN]-(a:Author) " +
        "RETURN collect(p.totalIncomingCitations) AS paperIncomingCitations, a.authorId AS authorId " +
        "ORDER BY a.authorId"
    ))
    for record in results:
        paperIncomingCitations = record["paperIncomingCitations"]
        authorHIndex = h_index(paperIncomingCitations)
        author = {
        	'authorId': record["authorId"],
        	'authorHIndex': authorHIndex
        }
        session.write_transaction(add_author_hindex, author)
	print("--- done")
	##########################

driver.close()