--droping all the tasks 
create or replace procedure drop_all()
returns string not null
language javascript
as
$$  
    var counter = 0;
    var tsk = "";
    var sql_txt = "show tasks";
    var sql_c1 = snowflake.createStatement({sqlText:sql_txt});
    var sql_txt2 = 'select "name" from table(result_scan(last_query_id()))';
    var sql_c2 = snowflake.createStatement({sqlText:sql_txt2});
    var res1 = sql_c1.execute();
    var last_row = res1.getRowCount();
    
    while(counter < last_row){
        sql_c1.execute();
        var res2 = sql_c2.execute();
        res2.next();
        tsk=res2.getColumnValue(1);
        var sql_ins = 'Drop task '+ tsk;
        try{
        var res3 = snowflake.createStatement({sqlText:sql_ins}).execute();
        }
        catch(err){
        
            return "Failed: "+ err;
        }
        counter = counter+1;
    }
    return counter;
$$;

--time travel dynamic
create or replace procedure time_proc(num varchar, DB varchar, Sch varchar, Tab varchar)
returns string not null
language Javascript
as 
$$
    var final = "";
    var state = "select name, cid, price from "+DB+"."+SCH+"."+TAB+ " before(offset => -60 *"+ NUM+ ")";
    var res = snowflake.createStatement({sqlText: state}).execute();
    while(res.next()){
    final = final + res.getColumnValue(1) +","+ res.getColumnValue(2) +","+ res.getColumnValue(3)+ "\n"; 
    }
    return final;
$$;
    
  
 --local variables and binds

create or replace procedure insert_proc()
returns varchar not null
language javascript
as
$$
    var id = 1;
    var name = 'Suresh';
    var address = 'Uttar Pradesh';
    var sql_stat = 'insert into emp values(:1,:2,:3)';
    var res = snowflake.createStatement({sqlText: sql_stat, binds:[id,name, address]}).execute();
    return "Success"
$$;

---Parameter and binds
create or replace procedure pram_ins(eid float, ename string, eaddress string)
returns FLOAT not null 
language javascript
as 
$$
    var sql_stat = 'insert into emp values(:1,:2,:3)';
    var res = snowflake.createStatement({sqlText: sql_stat,binds: [EID, ENAME, EADDRESS]}).execute();
    return 1;
$$;


