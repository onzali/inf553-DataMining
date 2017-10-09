import csv
import sys
import numpy as np

def main():
    try:
        inputFile=sys.argv[1]
        n = int(sys.argv[2])
        m = int(sys.argv[3])
        f = int(sys.argv[4])
        iterations = int(sys.argv[5])

	m1=[[0 for i in xrange(m)] for i in xrange(n)]        
        mat = open(inputFile)
        csv_f = csv.reader(mat)
        
	for row in csv_f:
            i,j,element=int(row[0])-1,int(row[1])-1,int(row[2])
            m1[i][j]=element
        
	u=[[1 for i in xrange(f)] for i in xrange(n)]
        v=[[1 for i in xrange(m)] for i in xrange(f)]
        
        for it in xrange(iterations):
            for r in xrange(n):
                for s in xrange(f):
                    x=u[r][s]
                    denom=0
                    numer=0
                    for j in xrange(m):
                        if(m1[r][j]>0):
                            suma=0.0
                            for k in xrange(f):
                                if(k!=s):
                                    suma+=u[r][k]*v[k][j]
                            numer+=v[s][j]*(m1[r][j]-suma)
                            denom+=v[s][j]*v[s][j]
                        x=float(numer)/float(denom)
                    u[r][s]=x

            for r in xrange(f):
                for s in xrange(m):
                    y=v[r][s]
                    if(y!=0):
                        denom=0
                        numer=0
                        for i in xrange(n):
                            sum=0.0
                            for k in xrange(f):
                                if(k!=r):
                                    sum+=u[i][k]*v[k][s]
                            if(m1[i][s]>0):
                                numer+=u[i][r]*(m1[i][s]-sum)
                                denom+=u[i][r]*u[i][r]
                            y=float(numer)/float(denom)
                    v[r][s]=y
            mDash=np.dot(u,v)
            count=0
            suma=0
            for i in xrange(n):
                for j in xrange(m):
                    if(m1[i][j]!=0):
                        count+=1
                        diff=(m1[i][j]-mDash[i][j])
                        suma+=diff*diff
            msre=(float(suma)/count)**0.5
            print("%.4f"%msre)
    except Exception,e:
        print(str(e))
        print('Syntax:')
        print('\tpython Onzali_Suba_uv.py <inputFile> n m f iterations')
        sys.exit()

if __name__ == "__main__":
    main()
