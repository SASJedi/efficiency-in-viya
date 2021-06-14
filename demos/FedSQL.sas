/* proc FedSQL */
/* Compute server operations */

/* Remerging summary statistics */
proc sql;
select Make, Model, MPG_Highway/mean(mpg_highway) as MPGRating
   from sashelp.cars
   having calculated MPGRating>1.5
   ;
quit;

/* Remerge doesn't work in FedSQL, and FedSQL can't read concateneated libref */
proc FedSQL;
select Make, Model, MPG_Highway/mean(mpg_highway) as MPGRating
   from sashelp.cars
   having calculated MPGRating>1.5
   ;
quit;

/* Get the data into a non-concatenated library */
proc copy in=sashelp out=sas;
   select cars;
run;

/* Create a query that generates the desired overall summary statistic */
proc FedSQL;
select mean(mpg_highway) as MPGMean
   from sas.cars
   ;
quit;

/* Cartesian join the summary statistic subquery to the detail data*/
proc FedSQL;
select Make, Model, MPG_Highway/MPGMean as MPGRating
   from sas.cars, (select mean(mpg_highway) as MPGMean from sas.cars) as mean
   where MPG_Highway/MPGMean>1.5
   ;
quit;

/* Working with DBMS data */
/* Investigating PROC SQL pushdown */
options sastrace=",,,d" sastraceloc=log nostsuffix;

proc SQL outobs=10;
select bar.GroupID, ID, Count, Norm1, txt200 
    from
        (select GroupID, count(*) as Count
		   from db.wide  
		   where GroupID in (1,5,9) 
             and Norm1>1000
		   group by 1
           ) as foo
       inner join 
	   sas.wide as bar
	   on foo.groupID = bar.groupID 
;
quit;

/* SASTRACE works for LIBNAME engine queries, but FedSQL uses different driver */
/* For FedSQL use IPTRACE, _METHOD, and _DIAG (extra info for Tech Support) */
options sastrace=off;
proc FedSQL 
/*    noexec */
/*    _method  */
/*    iptrace */
/*    _diag */
;
select bar.GroupID, ID, Count, Norm1, txt200 
    from
        (select GroupID, count(*) as Count
		   from db.wide  
		   where GroupID in (1,5,9) 
             and Norm1>1000
		   group by 1
           ) as foo
       inner join 
	   sas.wide as bar
	   on foo.groupID = bar.groupID 
    limit 10
;
quit;

/* FedSQL - working with CAS tables */
/* When running in CAS, no access to compute server data. */
proc fedSQL sessref=cs;
select bar.GroupID, ID, Count, Norm1, txt200 
    from
        (select GroupID, count(*) as Count
		   from db.wide  
		   where GroupID in (1,5,9) 
             and Norm1>1000
		   group by 1
           ) as foo
       inner join 
	   sas.wide as bar
	   on foo.groupID = bar.groupID 
    limit 10
;
quit;

/* All tables must either be in Compute Server or in CAS */
/* You CAN point a caslib to the same path location as a SAS libref... */
caslib cas2sas DATASOURCE= (SRCTYPE="path") path="%qsysfunc(pathname(sas))" libref=sas2cas;

/* And server-side load of a sas7bdat file is pretty fast */
proc casutil;
   load casdata="wide.sas7bdat" incaslib="cas2sas" 
         casout="wide"         outcaslib="cas2sas" replace;
run;

/* List DBMS tables in STUDENT schema */
proc casutil ;
    list files incaslib="db2cas" DATASOURCEOPTIONS=(SCHEMA="STUDENT");
run;

/* Now an in-memory copy of the SAS data set is available in CAS */
/* Note that you don't have to preload DBMS tables to the CASLIB */
proc fedSQL sessref=cs;
select bar.GroupID, ID, Count, Norm1, txt200 
    from
        (select GroupID, count(*) as Count
		   from db2cas.wide  
		   where GroupID in (1,5,9) 
             and Norm1>1000
		   group by 1
           ) as foo
       inner join 
	   cas2sas.wide as bar
	   on foo.groupID = bar.groupID 
    limit 10
;
quit;

/* You also run FedSQL in CAS using CAS actions */
proc cas;
/* A source block named myQuery (just your SQL code goes here) */
source myQuery;
select bar.GroupID, ID, Count, Norm1, txt200 
    from
        (select GroupID, count(*) as Count
		   from casuser.wide  
		   where GroupID in (1,5,9) 
             and Norm1>1000
		   group by 1
           ) as foo
       inner join 
	   cas2sas.wide as bar
	   on foo.groupID = bar.groupID 
;
endsource;
run;
/* Use the fedSQL.execDirect action to execute the code block */
fedSQL.execDirect /
   query=myQuery;
run;
quit;

/* FedSQL with Excel files */
LIBNAME xl xlsx "&path/data/customers.xlsx";

proc SQL;
select count (*) 
   from xl.my_customer_ids
;
quit;

/* FedSQL can't read xl librefs - there is no driver for PCFiles */
proc FedSQL;
select count (*) 
   from xl.my_customer_ids
;
quit;

/* If the XL file is not in a folder accessible to CAS, but is asseccible
   to the Compute Server, you can do a client-side load from the SAS libref */

PROC CASUTIL ;
LOAD DATA=XL.MY_CUSTOMER_IDS 
     OUTCASLIB="casuser" CASOUT="my_customer_IDs";
run;
LIBNAME xl clear;

proc FedSQL sessref=cs;
select count (*) 
   from casuser.my_customer_ids
;
quit;

/* If the XL file is in a folder accessible to CAS, you can do a server-side load */
PROC CASUTIL ;
LOAD CASDATA="customers.xlsx" incaslib="cas2sas"
     OUTCASLIB="cas2sas" CASOUT="my_customer_IDs";
run;

proc FedSQL sessref=cs;
select count (*) 
   from cas2sas.my_customer_ids
;
quit;

proc FedSQL sessref=cs;
select c.customer_id
      ,count (*) as Orders
    from casuser.my_customer_ids as c
       inner join 
         db2cas.big_order_fact as o
    on c.customer_id=o.customer_id
    group by c.Customer_ID
;
quit;