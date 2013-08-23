class DBDataObj
@resultset=0
@exception=""
@retval=0
def initialize()
end #initialize

attr_writer:retval
attr_reader:retval

attr_writer:resultset
attr_reader:resultset

attr_writer:exception
attr_reader:exception

end #DBDataObj

class DBAccess
  @conn=nil
 # @returnval=DBDataObj.new
  def initialize(conn)
   @conn=conn
  end

  def doExec(sql)
    returnval=DBDataObj.new
    if  @conn !=nil
       begin
       @conn.exec(sql)
       rescue PGError => exception
       returnval.resultset=(nil)
       returnval.exception=(exception)
       returnval.retval=(-1)
       return returnval
       end# begin
    else
       returnval.resultset=(nil)
       returnval.exception=("No used database connection.")
       returnval.retval=(-1)
       return returnval
    end#   if  @conn !=nil
    returnval.resultset=(nil)
    returnval.exception=("")
    returnval.retval=(0)
    return returnval
  end # doExec

  def doQuery(sql)
     returnval=DBDataObj.new
     if @conn !=nil
       begin
         result=@conn.exec(sql)
         puts "Successful get data..."
         rescue PGError => exception
         #puts exception.to_s
         returnval.resultset=(nil)
         returnval.exception=(exception)
         returnval.retval=(-1)
         return returnval
       end# begin
     else
         returnval.resultset=(nil)
         returnval.exception=("No used database connection.")
         returnval.retval=(-1)
         return returnval
     end  #if @conn !=nil
      returnval.resultset=(result)
      returnval.exception=("")
      returnval.retval=(0)
   # @result.each { |row| puts "%s | %-16s" %row.values_at('title','start_time')}
     return returnval
  end #doQuery

end #DBAccess


f1=DBPool.new()
f2=DBConnGuard.new(f1,5)

f3=f2.getConn()

if f3 ==nil
 puts "No used connection..."
 return
end

sql ="SELECT title,start_time FROM  songss"
ex=""
f4=DBAccess.new(f3)
f5=f4.doQuery(sql)


if f5.retval()==0
 puts "     title | start_time    "
 f5.resultset().each {| row|
    puts " %s | %-16s " %
      row.values_at('title', 'start_time')
  }
else
  puts "exception:"+f5.exception().to_s

end


sql1="create tables luowq ( name character varying(50) ,department_id integer ) "
f6=f4.doExec(sql1)

if f6.retval()==0
 puts "exec successfully"
else
 puts f6.exception().to_s
end
