/* Joins and Merges using DBMS and CAS data */
/* Prepare some data */
data sas.carMPG (keep=ID MPG: )
     db.carMPG (keep=ID MPG: )
     ;
     ID+1;
     set sashelp.cars;
run;

data sas.cars (drop=MPG: )
     ;
     ID+1;
     set sashelp.cars;
     format ID z3.;
run;
proc sort data=sas.cars;
   by Make Model;
run;

data casuser.cars;
   set sas.cars;
run;

/* Using SAS data sets */
data work.merged;
   merge sas.cars 
         sas.carMPG;
   by ID;
run;
/*  Need to sort! BY variables are not properly sorted on data set SAS.CARS. */
proc sort data=sas.cars out=work.cars;
   by id;
run;

data work.merged;
   merge work.cars 
         sas.carMPG;
   by ID;
run;

proc print data=work.merged(obs=5);
run;

/* NO need to sort CAS or DBMS tables */
data work.merged;
   merge casuser.cars 
         db.carMPG;
   by ID;
run;

proc print data=work.merged(obs=5);
run;

/* This DATA step runs on the Compute Server despite sessref=cs*/
data work.merged / sessref=cs;
   merge casuser.cars 
         db.carMPG;
   by ID;
run;

/* To run DATA step in CAS, CAS engine librefs must be used for all data sets */
/* Load Oracle tables to db2cas caslib memory */
proc casutil;
   load casdata="CARMPG"  incaslib="db2cas" 
         casout="carMPG"  outcaslib="db2cas" replace;
run;

/* Write output to casuser.merged*/
data casuser.merged /sessref=cs;
   merge casuser.cars 
         db2cas.carMPG;
   by ID;
run;

/* Drat! The ID is formatted differently in the two tables */
/* CAS distributes data to workers based on the FORMATTED vaues of the BY variables.*/
ods select variables;
proc contents data=casuser.cars (keep=ID);
run;
ods select variables;
proc contents data=db2cas.carMPG (keep=ID);
run;

/* The in-memory table is not the original - just a copy. I can fix the format */
proc casutil;
   altertable casdata="CARMPG"  incaslib="db2cas" 
         COLUMNS={{NAME="ID",FORMAT="z3."}};
run;

/* Now my merge works fine */
data casuser.merged / sessref=cs;
   merge casuser.cars 
         db2cas.carMPG;
   by ID;
run;

proc print data=casuser.merged(obs=5);
run;

/* What about a FedSQL join? */
/* Let's restore the format to the way it was */
proc casutil;
   altertable casdata="CARMPG"  incaslib="db2cas" 
         COLUMNS={{NAME="ID",FORMAT=""}};
run;

ods select variables;
proc contents data=casuser.cars (keep=ID);
run;
ods select variables;
proc contents data=db2cas.carMPG (keep=ID);
run;

proc fedSQL sessref=cs;
create table casuser.joined as
select c.*, MPG_City, MPG_Highway 
   from casuser.cars as c
   inner join 
        db2cas.carmpg as m
   on c.ID=m.ID
;
quit;

/* Oooh - that was much easier! */
proc fedSQL sessref=cs;
title "Joined";
select * from casuser.joined order by id limit 5;
title "Merged";
select * from casuser.merged order by id limit 5;
run;

/* It even works in the CAS action */
proc delete data=casuser.joined;
run;

proc cas;
source myQ;
create table casuser.joined as
select c.*, MPG_City, MPG_Highway 
   from casuser.cars as c
   inner join 
        db2cas.carmpg as m
   on c.ID=m.ID
;
endsource;
run;
fedSQL.execDirect /
   query=myQ
;
run;
quit;

proc print data=casuser.joined(obs=5);
run;

/* FedSQL joins of DBMS tables */
/* Compute Server - impicit pass-through */
proc fedSQL;
select
        Country 
      , Count(*) as Nobs
      , MIN(TOTAL_RETAIL_PRICE) as Min
      , AVG(TOTAL_RETAIL_PRICE) as Mean
		, MAX(TOTAL_RETAIL_PRICE) as Max
     from db.BIG_ORDER_FACT o
          inner join 
          db.BIG_CUSTOMER_DIM c
     on o.Customer_ID=c.Customer_ID
     where weekday(datepart(order_date)) = '7'
     group by COUNTRY
     order by COUNTRY
;
quit;

/* Compute Server - explicit pass-through */
proc fedSQL;
select * 
   from connection to db
   (
	 select /*+full parallel(8)*/ 
	        Country 
	      , Count(*) as Nobs
	      , MIN(TOTAL_RETAIL_PRICE) as Min
	      , AVG(TOTAL_RETAIL_PRICE) as Mean
			, MAX(TOTAL_RETAIL_PRICE) as Max
	     from student.BIG_ORDER_FACT o
	          inner join 
	          student.BIG_CUSTOMER_DIM c
	     on o.Customer_ID=c.Customer_ID
	     where to_char(order_date,'d') = '7'
	     group by COUNTRY
	     order by COUNTRY
    )
;
quit;

options sastrace=",,,d" sastraceloc=saslog nostsuffix;
proc sql;
select  Country 
      , Count(*) as Nobs
      , MIN(TOTAL_RETAIL_PRICE) as Min
      , AVG(TOTAL_RETAIL_PRICE) as Mean
		, MAX(TOTAL_RETAIL_PRICE) as Max
     from db.BIG_ORDER_FACT o
          inner join 
          db.BIG_CUSTOMER_DIM 	c
     on o.Customer_ID=c.Customer_ID
     where weekday(datepart(order_date)) = 7
     group by COUNTRY
     order by COUNTRY
;
quit;
options sastrace=off stsuffix nofullstimer;

proc FedSQL _method noexec;
select  Country 
      , Count(*) as Nobs
      , MIN(TOTAL_RETAIL_PRICE) as Min
      , AVG(TOTAL_RETAIL_PRICE) as Mean
		, MAX(TOTAL_RETAIL_PRICE) as Max
     from db.BIG_ORDER_FACT o
          inner join 
          db.BIG_CUSTOMER_DIM 	c
     on o.Customer_ID=c.Customer_ID
     where weekday(datepart(order_date)) = 7
     group by COUNTRY
     order by COUNTRY
;
quit;

proc FedSQL IPTRACE noexec;
select  Country 
      , Count(*) as Nobs
      , MIN(TOTAL_RETAIL_PRICE) as Min
      , AVG(TOTAL_RETAIL_PRICE) as Mean
		, MAX(TOTAL_RETAIL_PRICE) as Max
     from db.BIG_ORDER_FACT o
          inner join 
          db.BIG_CUSTOMER_DIM 	c
     on o.Customer_ID=c.Customer_ID
     where weekday(datepart(order_date)) = 7
     group by COUNTRY
     order by COUNTRY
;
quit;

/* In CAS, note that FedSQL does not require DBMS 
   tables to be pre-loaded to CAS memory */

/* CAS - implicit pass-through */
proc fedSQL sessref=cs;
select
        Country 
      , Count(*) as Nobs
      , MIN(TOTAL_RETAIL_PRICE) as Min
      , AVG(TOTAL_RETAIL_PRICE) as Mean
		, MAX(TOTAL_RETAIL_PRICE) as Max
     from db2cas.BIG_ORDER_FACT o
          inner join 
          db2cas.BIG_CUSTOMER_DIM c
     on o.Customer_ID=c.Customer_ID
     where weekday(datepart(order_date)) = '7'
     group by COUNTRY
     order by COUNTRY
;
quit;

/* CAS - explicit pass-through */
proc fedSQL sessref=cs;
select * 
   from connection to db2cas
   (
	 select /*+full parallel(8)*/ 
	        Country 
	      , Count(*) as Nobs
	      , MIN(TOTAL_RETAIL_PRICE) as Min
	      , AVG(TOTAL_RETAIL_PRICE) as Mean
			, MAX(TOTAL_RETAIL_PRICE) as Max
	     from student.BIG_ORDER_FACT o
	          inner join 
	          student.BIG_CUSTOMER_DIM c
	     on o.Customer_ID=c.Customer_ID
	     where to_char(order_date,'d') = '7'
	     group by COUNTRY
	     order by COUNTRY
    )
;
quit;
