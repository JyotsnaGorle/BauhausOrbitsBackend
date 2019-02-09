from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os
# Importing os and glob to find all PDFs inside subfolder
import glob
import codecs
g_login = GoogleAuth()
g_login.LocalWebserverAuth()
drive = GoogleDrive(g_login)
types_of_encoding = ["utf8", "cp1252"] 
os.chdir("docs")
for file in glob.glob("*.pdf"):
    print(file)
    for encoding_type in types_of_encoding:
	    with open(file, mode='r', errors='replace', encoding=encoding_type) as f:
	    	fn = os.path.basename(f.name)
	    	text = f.read()
	    	# fileDrive = drive.CreateFile({'title': fn})
	    	# fileDrive.SetContentString(f.read())
	    	# fileDrive.Upload()
	    	print("file uploaded" + fn)
	    	print(text)