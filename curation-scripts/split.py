import os
import time
import shutil
from PyPDF2 import PdfFileWriter, PdfFileReader
# loop through a folder and read each file.
rootdir = 'C:/Users/Rahul/Documents/bauhausOrbits/___bauhausbuecher/pdfs';
listOdDirectories = [];
pdfFiles = [];
for subdir, dirs, files in os.walk(rootdir):
    for file in files:
        f = open(rootdir+'/'+file, "rb")
        inputpdf = PdfFileReader(f);
# create new folder for each book and the for each book -
        dirName = rootdir+'/'+file[:-4];
        os.mkdir(dirName)
        listOdDirectories.append(file[:-4])
        # get each page for a new pdf
        for i in range(inputpdf.numPages):
            output = PdfFileWriter()
            output.addPage(inputpdf.getPage(i))
            with open(rootdir+'/'+file[:-4]+"%s.pdf" % i, "wb") as outputStream:
                output.write(outputStream)
        f.close()

for eachFile in os.listdir(rootdir):
    if eachFile.endswith('.pdf'):
        pdfFiles.append(eachFile)

for eachPDFfile in pdfFiles:
    for eachDir in listOdDirectories:
        if eachDir in eachPDFfile:
            print(eachPDFfile)
            shutil.move(rootdir+'/'+eachPDFfile, rootdir+'/'+eachDir)