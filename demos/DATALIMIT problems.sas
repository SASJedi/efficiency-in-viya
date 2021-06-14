/**********************************************************
 Setup:
 Create a table with 10 million rows 
**********************************************************/
data casuser.products;
   call streaminit(99);
    do i=1 to 10000000;
        x=rand('normal');
        if x<.10 then Product="A";
            else if x<.30 then Product="B";
            else if x<.60 then Product="C";
            else Product="D"; 
        Quantity=round(rand('uniform',1,100));
        output;
    end;
    drop x i;
run;

/**********************************************************
 Task: Graph a very large dataset
 - Create a vertical bar chart of the 10 million row CAS table
 - must override the SGPLOT data size limit
**********************************************************/
ods graphics / maxobs=10000000;
proc sgplot data=casuser.products;
    vbar Product / 
           response=Quantity 
           stat=sum categoryorder=respdesc;
    format Quantity comma16.;
run;
ods graphics / reset;

/**********************************************************
 Problem: CASDATALIMIT prevents downloading enormous data
**********************************************************/

/**********************************************************
 Solution (not recommended): Override the CASDATALIMIT 
 Allows transfer of more data from CAS to Compute Server
 This works, but runs way too long (15 seconds) 
**********************************************************/
/* Find out hom big the data is */
proc casutil;
    contents casdata="products" incaslib="casuser";
quit;

ods graphics / maxobs=10000000;
/* USe the DATALIMIT= data set option to override the system limit */
proc sgplot data=casuser.products(datalimit=160000000);
    vbar Product / 
           response=Quantity 
           stat=sum categoryorder=respdesc;
    format Quantity comma16.;
run;
ods graphics / reset;

/**********************************************************
 Better solution: 
 a. Summarize the data in CAS and create a CAS table    
 b. Graph the summarized CAS table
**********************************************************/

proc cas;
* Summarize the CAS table with an action and save the results as a CAS table *;
simple.summary /
    table={name="products"
          ,caslib="casuser"
          ,groupBy="Product"
           }
   ,input="Quantity"
   ,subSet={"SUM"}
   ,casOut={name="products_sum"
                 ,caslib="casuser"
                 ,replace=TRUE
           }
;
quit;

* b *;
proc sgplot data=casuser.products_sum;
    vbar Product / 
           response=_sum_
           categoryorder=respdesc;
    format _sum_ comma16.;
    label _sum_="Total Quantity";
quit;

/**********************************************************
 Cleanup: Drop tabls no longer needed
**********************************************************/
proc casutil;
droptable casdata="products" incaslib="casuser" quiet;
droptable casdata="products_sum" incaslib="casuser" quiet;
quit;