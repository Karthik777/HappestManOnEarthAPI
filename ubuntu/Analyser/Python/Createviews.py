import couchdb
from couchdb.design import ViewDefinition

class TweetStore(object):
        def __init__(self, url):
            self.url = url        
        def makeview1(self):
                self.server = couchdb.Server(self.url)
                self.db = self.server['perth']
                self._create_views_full_doc(self.db)
        def makeview2(self):
                self.server = couchdb.Server(self.url)
                self.db = self.server['perth']
                self._creat_views_groupviews(self.db)
        def makeview3(self):
                self.server = couchdb.Server(self.url)
                self.db = self.server['perth']
                self._creat_views_groupviews2(self.db)                 
        def _creat_views_groupviews(self,db):
              self.db = db
              count_map = 'function(doc){ var createdyear = doc.created_at.split(" ")[5]; var timestring = doc.created_at.split(" ")[3]; var thour = timestring.split(":")[0]; var thour = parseInt(thour); var tmin = timestring.split(":")[1]; var intmin = parseInt(tmin); if(intmin > 30) thour++; if(thour == 25){ thour = 1;} else { thour = thour%24 } emit([doc.user.id, doc.sentiment, createdyear, thour],1); }'        
              count_reduce = 'function(keys, values, rereduce){ return sum(values); }'
              view = couchdb.design.ViewDefinition('groupview', 'grouview', count_map, reduce_fun=count_reduce)
              view.sync(self.db)
        def _creat_views_groupviews2(self,db):
              self.db = db
              count_map = 'function(doc){ var createdyear = doc.created_at.split(" ")[5]; var timestring = doc.created_at.split(" ")[3]; var thour = timestring.split(":")[0]; var thour = parseInt(thour); var tmin = timestring.split(":")[1]; var intmin = parseInt(tmin); if(intmin > 30) thour++; if(thour == 25){ thour = 1;} else { thour = thour%24 } emit([doc.sentiment, createdyear, thour],1); }'        
              count_reduce = 'function(keys, values, rereduce){ return sum(values); }'
              view = couchdb.design.ViewDefinition('groupview', 'grouview2', count_map, reduce_fun=count_reduce)
              view.sync(self.db)      
        def _create_views_full_doc(self,db):
              self.db = db
              count_map = 'function(doc){ if (doc.coordinates !== null) emit([doc]); }'
              count_reduce = ''
              view = couchdb.design.ViewDefinition('fulldoc', 'fulldoc', count_map, reduce_fun=count_reduce)
              view.sync(self.db)

InitialCreateviews = TweetStore('http://localhost:5984/')
InitialCreateviews.makeview1()
InitialCreateviews.makeview2()
InitialCreateviews.makeview3()
