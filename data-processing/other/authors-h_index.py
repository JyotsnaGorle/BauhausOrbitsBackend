from neo4j.v1 import GraphDatabase
from neo4j.v1 import CypherError

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
    
    print(".. author updated: " + str(author['authorId']))

with driver.session() as session:

    results = list(session.run(
        "MATCH (p:Paper)<-[w:AUTHOR_IN]-(a:Author) " +
        "RETURN collect(p.totalIncomingCitations) AS paperIncomingCitations, a.authorId AS authorId " +
        "ORDER BY a.authorId"
    ))
    
    for record in results:
        paperIncomingCitations = record["paperIncomingCitations"]
        
        authorId = record["authorId"]
        authorHIndex = h_index(paperIncomingCitations)
        
        author = {}
        author["authorId"] = authorId
        author["authorHIndex"] = authorHIndex
        
        session.write_transaction(add_author_hindex, author)

driver.close()