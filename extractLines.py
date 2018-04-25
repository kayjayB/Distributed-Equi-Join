inputfile = open('data/lineitem.txt','r+')
linesOfFile=inputfile.readlines()

linesOfFile = linesOfFile[0:1000000]

outputfile = open('data/1milLines1.txt', 'w')
outputfile.writelines(linesOfFile)

inputfile = open('data/lineitem2.txt','r+')
linesOfFile=inputfile.readlines()

linesOfFile = linesOfFile[0:1000000]

outputfile = open('data/1milLines2.txt', 'w')
outputfile.writelines(linesOfFile)