import os.path
import shutil
import os
import pandas as pd

class Utils(object):
    
    def __init__(self):
	pass

    # Transform neo4j results of type records to a dataframe
    def records_to_df(self, records):
        final_dict = {}
        i = 0
        df = ""

        # loop records
        for record in records:
            if "re" in record:
                re = record["re"]
                attrs = re.properties

                final_dict[i] = {
                    "id": re.id
                }

                # save all attrs of this record
                for k,v in attrs.items():
                    final_dict[i][k] = v
                i = i + 1
            else:
                print("Must name the records 're'")

        if i > 0:
            df = pd.DataFrame.from_dict(final_dict, "index")
            df = df.fillna(value='')

        return df
