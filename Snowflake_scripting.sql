----------Store procedure--------

create or replace procedure sqr_area()
    returns varchar 
    language sql
    as
    $$
            declare
                side float;
                area float;
            begin
                side := 4;
                area := side*side;
                return area;
            end;
    $$;
    
call  sqr_area()

--if we want to run only the block and do not want to create a procedure
--anonymous block

execute immediate $$

-- Snowflake Scripting code
declare
  side float;
  area float;
begin
  side := 3;
  area := side*side;
  return area;
end;

$$
;

--or we can use session variable to store the block code

set code = 

$$
declare 
    side float;
    area float;
begin
    side :=  3;
    area := side*side;
    return area;
end;
$$;


execute immediate $code


--creating tables in block

execute immediate
$$

begin
  create table parent (id integer);
  create table child (id integer, parent_id integer);
  return 'Completed';
end
$$;

--declare a variable


execute immediate $$
declare 
    profit  number default 0.0;
Begin
    let cost number := 80;
    let revenue number default 110;
    profit := revenue - cost;
    return profit;
end;
$$;



----Examples

execute immediate
$$
Declare
    w integer;
    x integer;
    dt date;
    result varchar;
Begin
    w := 1;
    w := 24*7;
    dt := '2020-09-30'::date;
    result := w::varchar||','||dt::varchar;
    return result;
end;
$$;
