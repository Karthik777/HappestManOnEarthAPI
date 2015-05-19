import couchdb

server = couchdb.Server('http://localhost:5984')
db = server['perth']
testview_list = db.view('groupview/groupview2', group_level=3)

Matrix2010 = [[0 for x in range(24)] for x in range(3)] 
Matrix2011 = [[0 for x in range(24)] for x in range(3)] 
Matrix2012 = [[0 for x in range(24)] for x in range(3)] 
Matrix2013 = [[0 for x in range(24)] for x in range(3)] 
Matrix2014 = [[0 for x in range(24)] for x in range(3)] 
Matrix2015 = [[0 for x in range(24)] for x in range(3)] 
Matrix10 = [[0 for x in range(24)] for x in range(3)] 
Matrix11 = [[0 for x in range(24)] for x in range(3)] 
Matrix12 = [[0 for x in range(24)] for x in range(3)] 
Matrix13 = [[0 for x in range(24)] for x in range(3)] 
Matrix14 = [[0 for x in range(24)] for x in range(3)] 
Matrix15 = [[0 for x in range(24)] for x in range(3)]
total = [0,0,0,0,0,0]    
totalloop = 0


for r in testview_list:
    totalloop += 1
    year = int(r.key[1])
    value = int(r.value)

    hour = int(r.key[2])
    if year == 2010:
        if r.key[0] == "Negative":
            Matrix2010[0][hour] += value
        elif r.key[0] == "Neutral":
            Matrix2010[1][hour] += value
        else:
            Matrix2010[2][hour] += value
    elif year == 2011:
        if r.key[0] == "Negative":
            Matrix2011[0][hour] += value
        elif r.key[0] == "Neutral":
            Matrix2011[1][hour] += value
        else:
            Matrix2011[2][hour] += value
    elif year == 2012:
        if r.key[0] == "Negative":
            Matrix2012[0][hour] += value
        elif r.key[0] == "Neutral":
            Matrix2012[1][hour] += value
        else:
            Matrix2012[2][hour] += value
    elif year == 2013:
        if r.key[0] == "Negative":
            Matrix2013[0][hour] += value
        elif r.key[0] == "Neutral":
            Matrix2013[1][hour] += value
        else:
            Matrix2013[2][hour] += value
    elif year == 2014:
        if r.key[0] == "Negative":
            Matrix2014[0][hour] += value
        elif r.key[0] == "Neutral":
            Matrix2014[1][hour] += value
        else:
            Matrix2014[2][hour] += value
    elif year == 2015:
        if r.key[0] == "Negative":
            Matrix2015[0][hour] += value
        elif r.key[0] == "Neutral":
            Matrix2015[1][hour] += value
        else:
            Matrix2015[2][hour] += value




for i in range(0,24):
    for j in range(0,3):
        total[0] += Matrix2010[j][i]
        total[1] += Matrix2011[j][i]
        total[2] += Matrix2012[j][i]
        total[3] += Matrix2013[j][i]
        total[4] += Matrix2014[j][i]
        total[5] += Matrix2015[j][i]
    
    for k in range(0,3):
        if Matrix2010[k][i] > 0:
            Matrix10[k][i] = Matrix2010[k][i]
            Matrix2010[k][i] = int(float(float(Matrix2010[k][i])/float(total[0]))*100)
        if Matrix2011[k][i] > 0:
            Matrix11[k][i] = Matrix2011[k][i]    
            Matrix2011[k][i] = int(float(float(Matrix2011[k][i])/float(total[1]))*100)
        if Matrix2012[k][i] > 0:
            Matrix12[k][i] = Matrix2012[k][i]
            Matrix2012[k][i] = int(float(float(Matrix2012[k][i])/float(total[2]))*100)
        if Matrix2013[k][i] > 0:
            Matrix13[k][i] = Matrix2013[k][i]
            Matrix2013[k][i] = int(float(float(Matrix2013[k][i])/float(total[3]))*100)
        if Matrix2014[k][i] > 0:
            Matrix14[k][i] = Matrix2014[k][i]
            Matrix2014[k][i] = int(float(float(Matrix2014[k][i])/float(total[4]))*100)
        if Matrix2015[k][i] > 0:    
            Matrix15[k][i] = Matrix2015[k][i]
            Matrix2015[k][i] = int(float(float(Matrix2015[k][i])/float(total[5]))*100)
    

    for l in range(0,6):
        total[l] = 0


f = open('/home/ubuntu/Analyser/Result/Statistics.txt','w')

for y in range(2010,2016):
    for i in range(0,24):
        JsonStringData = "{ year:" + str(y) + ", hour: " + str(i) + ","
        if y == 2010:
            JsonStringData = JsonStringData + " Pos: " + str(Matrix2010[2][i]) + ", Neg: " + str(Matrix2010[0][i]) + ", Neut: " + str(Matrix2010[1][i]) 
            JsonStringData = JsonStringData + ", tPos: " + str(Matrix10[2][i]) + ", TNeg: " + str(Matrix10[0][i]) + ", TNeut: " + str(Matrix10[1][i]) + " } \n"
        elif y == 2011:
            JsonStringData = JsonStringData + " Pos: " + str(Matrix2011[2][i]) + ", Neg: " + str(Matrix2011[0][i]) + ", Neut: " + str(Matrix2011[1][i])
            JsonStringData = JsonStringData + ", tPos: " + str(Matrix11[2][i]) + ", TNeg: " + str(Matrix11[0][i]) + ", TNeut: " + str(Matrix11[1][i]) + " } \n"
        elif y == 2012:
            JsonStringData = JsonStringData + " Pos: " + str(Matrix2012[2][i]) + ", Neg: " + str(Matrix2012[0][i]) + ", Neut: " + str(Matrix2012[1][i])
            JsonStringData = JsonStringData + ", tPos: " + str(Matrix12[2][i]) + ", TNeg: " + str(Matrix12[0][i]) + ", TNeut: " + str(Matrix12[1][i]) + " } \n"
        elif y == 2013:
            JsonStringData = JsonStringData + " Pos: " + str(Matrix2013[2][i]) + ", Neg: " + str(Matrix2013[0][i]) + ", Neut: " + str(Matrix2013[1][i])
            JsonStringData = JsonStringData + ", tPos: " + str(Matrix13[2][i]) + ", TNeg: " + str(Matrix13[0][i]) + ", TNeut: " + str(Matrix13[1][i]) + " } \n"
        elif y == 2014:
            JsonStringData = JsonStringData + " Pos: " + str(Matrix2014[2][i]) + ", Neg: " + str(Matrix2014[0][i]) + ", Neut: " + str(Matrix2014[1][i])
            JsonStringData = JsonStringData + ", tPos: " + str(Matrix14[2][i]) + ", TNeg: " + str(Matrix14[0][i]) + ", TNeut: " + str(Matrix14[1][i]) + " } \n"
        else:                    
            JsonStringData = JsonStringData + " Pos: " + str(Matrix2015[2][i]) + ", Neg: " + str(Matrix2015[0][i]) + ", Neut: " + str(Matrix2015[1][i])
            JsonStringData = JsonStringData + ", tPos: " + str(Matrix15[2][i]) + ", TNeg: " + str(Matrix15[0][i]) + ", TNeut: " + str(Matrix15[1][i]) + " } \n"
            
        f.write(JsonStringData)    
    
f.close()        
print ("OK")
    
