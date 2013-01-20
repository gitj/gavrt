import numpy as np

class BasicDB():
    def __init__(self,**kwargs):
        if kwargs.has_key('rw'):
            self.rw = kwargs.pop('rw')
        else:
            self.rw = False
        self.connect(**kwargs)
        
    def connect(self, **kwargs):
        raise NotImplementedError("connect method must be implemented")
        
    def checkDB(self):
        try:
            self.db.commit()
        except:
            self.connect()
        self.db.commit()
        
    def cursor(self):
        self.checkDB()
        return self.db.cursor()
    
    def commit(self):
        return self.db.commit()
        
    def insertRecord(self,table,rec,update = False,keepid = False):
        self.checkDB()
        return insert_record(self.db, table, rec,update=update,keepid=keepid)
    def getLastId(self,table):
        self.checkDB()
        return get_last_id(self.db,table)
    def getLastRecord(self,table):
        self.checkDB()
        return get_last_record(self.db,table)
    def getRecordById(self,table,id,idname='ID'):
        self.checkDB()
        return get_record_by_id(self.db,table,id,idname=idname)    
    def get(self,*args,**kwargs):
        if kwargs.has_key('asfloat'):
            asfloat = kwargs['asfloat']
        else:
            asfloat = True
        self.checkDB()
        return get_as_dict(self.db,*args,**dict(asfloat=asfloat))

def insert_record(db,table,fields,update = False,keepid = False):
    """
    id returned is unreliable if update=True
    """
    
    keystr = ''
    valstr = ''
    values = []
    if not keepid:
        try:
            del fields['ID']
        except:
            pass
    for k,v in fields.items():
        keystr = keystr + '`'+k + '`, '
        valstr = valstr + '%s, '
        values.append(v)
    keystr = keystr[:-2]    #strip final ,
    valstr = valstr[:-2] 
    c = db.cursor()
    #print "INSERT INTO "+ table +" (" + keystr + ") VALUES (" + valstr + ");" , values
    if update:
        updatestr = 'ON DUPLICATE KEY UPDATE '
        vals = ['%s = VALUES(%s)' % (key,key) for key in fields.keys() if key != 'ID']
        updatestr += ', '.join(vals)
    else:
        updatestr = ''
    #print "INSERT INTO "+ table +" (" + keystr + ") VALUES (" + valstr + ") " + updatestr + ";"
    c.executemany(
             "INSERT INTO "+ table +" (" + keystr + ") VALUES (" + valstr + ") " + updatestr + ";",
             [tuple(values)])
    id = db.insert_id()
    db.commit()
#    print c._executed
    print "new record id",id
    return id

def get_as_rec(db,*args):
    c = db.cursor()
    print "executing"
    c.execute(*args)
    print "fetching"
    res = c.fetchall()
    print "toarray"
    res = np.array(res)
    dtypes = []
    for n,d in enumerate(c.description):
        if d[1] in [0,4,5,246]:     #Typecodes for Decimal,Float,Duble,NewDecimal
            dtypes.append((d[0],float))
        elif d[1] in [1,2,3,8,9]:
            dtypes.append((d[0],int))
        else:
            dtypes.append((d[0],object))
    resarr = np.empty((res.shape[0],),dtype=dtypes)
    for n,d in enumerate(c.description):
        resarr[d[0]] = res[:,n]
    return resarr
    

def get_as_dict(db,*args,**kwargs):
    try:
        asfloat = kwargs['asfloat']
    except:
        asfloat = True
    c = db.cursor()
    c.execute(*args)
    res = np.array(c.fetchall())
    if res.shape[0] == 0:
        return {}
    descr = [x[0] for x in c.description]
    if asfloat:
        rd = {}
        for x in range(res.shape[1]):
            try:
                r = res[:,x].astype('float')
            except:
                r = res[:,x]
            rd[descr[x]] = r
    else:
        rd = dict([(descr[x],res[:,x]) for x in range(res.shape[1])])
    return rd

def get_last_id(db,table):
    c = db.cursor()
    c.execute("SELECT ID FROM " + table + " ORDER BY ID DESC LIMIT 1;")
    return int(c.fetchone()[0])

def get_last_record(db,table):
    c = db.cursor()
    c.execute("SELECT * FROM " + table + " ORDER BY ID DESC LIMIT 1;")
    res = c.fetchone()
    descr = [x[0] for x in c.description]
    return dict(zip(descr,res))

def get_record_by_id(db,table,id,idname = 'ID'):
    c = db.cursor()
    c.execute("SELECT * FROM " + table + (" WHERE %s " % idname) + "= %s;",(id,))
    res = c.fetchone()
    descr = [x[0] for x in c.description]
    return dict(zip(descr,res))
