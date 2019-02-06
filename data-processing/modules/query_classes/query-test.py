from neo4j.v1 import GraphDatabase
from modules.utils.utils import Utils

class QueryAuthor(object):
    
    def __init__(self, neo4j_driver):
        self._driver = neo4j_driver
        self._utils = Utils()
    
    def close(self):
        self._driver.close()

    def get_all(self):
        with self._driver.session() as session:
            res = session.write_transaction(self._all)
            return res
    
    @staticmethod
    def _all(tx):
        res = tx.run(
            "MATCH (re:Author)-[w:AUTHOR_IN]->(p:Paper) "
            "WHERE w IS NOT NULL "
            "RETURN DISTINCT(re)"
        )
        return list(res)

## end