require 'rubygems'
require 'pg'
require 'thread'

class DBObj
  @isused
  @isok
  @db
  def initialize(isused,isok, db)
  @isused=isused
  @db= db
  @isok=isok
end #initialize

attr_writer :isused
attr_reader :isused

attr_writer :isok
attr_reader :isok

attr_writer :db
attr_reader :db

 end #DBObj


class DBPool
  @DBlist=nil
  @DBCount=20
  @m=nil
  @t=nil
  @isobs=0
  @isshutdown=0
  def  initialize()
  puts "Instance created..."
  @DBlist=Hash.new
  @m=Mutex.new
  end

  def init(dbcnt,ipaddr,port,dbname,username,passwd)
  puts "ip:"+ipaddr+",port:"+port.to_s+",dbname:"+dbname+",username:"+username+",passwd:"+passwd
  @DBCount=dbcnt
  retval= createDBList(ipaddr,port,dbname,username,passwd)
  if retval ==0
  puts "created "+@DBCount.to_s+" database connection string.."
  observeDBList()
  else
    dbClose()
  end
  return retval
  end

  def  dbClose
   @isshutdown=1
   threadJoin()
   @isobs=0  #finish observe
   
   @DBlist.each_pair do |key,value| 
     @DBlist[key].db().close
     @DBlist[key].isused=(0)
   end   #do, finish connection
   puts "Close all DB access strings..."
   end

 def  createDBList(ipaddr,port,dbname,username,passwd)
 no=0
 while  no < @DBCount do
 begin
 db=PG::Connection.new(ipaddr,port,nil,nil,dbname,username,passwd)
 rescue PGError => ex
 puts ex.to_s
 return ex
 end #begin
 isused=0
 isok=1
 dbobj=DBObj.new(isused,isok,db)
 @DBlist[no]=dbobj
 no+=1
 end #while
 puts "end createDBList"
 return 0
 end #createDBlist



 def getHandle(retrycnt)
 handle=-1
 @m.synchronize {
 while 1
     @DBlist.each_pair do |key,value|
       if value.isused() ==0 && value.isok() ==1
        handle=key
        value.isused=(1)
        break
       end # if
     end    #do
  if handle >=0
    break
  end #  if @handle >=0
  if retrycnt ==0
    break
  end #  retrycnt ==0
  if retrycnt!=-1
    retrycnt-=1
     end #  if retrycnt!=-1
  end  #while 1
     }
    return handle
    end #getHandle

  def releaseHandle(handle)
   @m.synchronize {
       if handle >=0 && handle< @DBCount
        @DBlist[handle].isused=(0)
        return 0
      else
        return -1
      end #if
     }
     end


 def getDBConn(handle)
   @m.synchronize {
          if ! (handle >=0 && handle <@DBCount)
          @conn=nil
        else
          @conn=@DBlist[handle].db()
          @DBlist[handle].isused=(1)
        end #if ! (handle >=0 && handle <@DBCount)
     }
      return @conn
    end #getDB


 def observeDBList
  puts "start ObserveDBList"
  $requestedToShutDown = false
   @t=Thread.new{
   @isobs=1
   while !$requestedToShutDown
      if @isshutdown ==1
        return 
      end
      @m.synchronize{
      puts "process ObserveDBList"
      @DBlist.each_pair do |key,value|
       puts "No.:"+key.to_s+",isused:"+value.isused().to_s+",isok:"+value.isok().to_s
       end #do
      }
       sleep 100
      end #while
    }
  puts "end ObserveDBList"
 end #ObserveDBList

  def threadJoin
    if @isobs>0
      @t.join()
     puts "Join thread"
    end
  end #threadJoin

end #DBPool


class DBConnGuard
  @dbpool=nil
  @handle=-1
  @conn=nil
  def initialize(pool,retrycnt)
      @dbpool=pool
      @handle=@dbpool.getHandle(retrycnt)
      @conn=@dbpool.getDBConn(@handle)
  end #initialize

  def getConn
    return @conn
    end

  def releaseConn
    if @handle>=0 && @conn!=nil
      @dbpool.releaseHandle(@handle)
     end
   puts "Release Handle..."
  end #releaseConn

end #DBAccessManager



puts "out of class :"
f1=DBPool.new
re=f1.init(20,"10.110.162.153",  5432,"garfielddb","jiafei","jiafei")
if re !=0
  puts re.to_s
  return
end

puts "out of class :1"
f2=DBConnGuard.new(f1,5)

f3=f2.getConn()

if f3 ==nil
 puts "No used connection..."
 return
end

puts "go on..."

f3.exec( "SELECT name,department_id FROM  luowq " ) do |result|
    puts "     title | start_time    "
  result.each do |row|
    puts " %s | %-16s " %
      row.values_at('title', 'start_time')
    end
    end


f2.releaseConn()
#f1.dbClose()
#f1.threadJoin()
puts "over..."
return
