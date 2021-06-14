/*************************************************************
PROC CASUTIL:
   Requires a CAS session
   Runs on the Compute Server, generates CASL that executes in CAS
   Perform the following actions:
    - List the in-memory tables already loaded in a caslib.
    - List the files in a caslib data source.
    - Load files from the caslib data source to in-memory tables.
    - View table column names, data types, and other column information.
    - Alter table name, column formats, etc.
    - Save CAS tables as files in the caslib data source. 
**************************************************************/

proc casutil ;
list tables incaslib="casuser";
run;
proc casutil ;
list files incaslib="casuser";
run;

/* No tables in db2cas */
proc casutil ;
list tables incaslib="db2cas";
run;
proc casutil ;
list files incaslib="db2cas";
run;
proc casutil ;
list files incaslib="db2cas" DATASOURCEOPTIONS=(SCHEMA="STUDENT");
run;

proc casutil;
   load data=sashelp.cars(where=(Make="Toyota")) casout="Toyotas" outcaslib="casuser" replace;
   save casdata="Toyotas" incaslib="casuser"     casout="toyotas.csv" outcaslib="casuser" replace; 
   list files incaslib="casuser";
   list tables incaslib="casuser";
   contents casdata="toyotas" incaslib="casuser";
run;

proc casutil;
    contents casdata="toyotas" incaslib="casuser";
    altertable casdata="Toyotas" incaslib="casuser" 
     /* rename="ToyotaCars" */
        drop={"Origin",'Type',"Length","Weight","Wheelbase"};
    contents casdata="Toyotas" incaslib="casuser";
run;

proc casutil;
   droptable casdata="Toyotas" incaslib="casuser";
   list files incaslib="casuser";
   list tables incaslib="casuser";
run;

proc casutil;
    load casdata="toyotas.csv" incaslib="casuser" casout="Toyotas" outcaslib="casuser" replace;
    contents casdata="Toyotas" incaslib="casuser";
run;

proc casutil;
    altertable casdata="Toyotas" incaslib="casuser" 
        rename="ToyotaCars" COLUMNS={{NAME="MSRP",FORMAT="COMMA12.2"},{NAME="INVOICE",FORMAT="COMMA12.2"}};
run;

proc casutil;
    load casdata="toyotas.csv" incaslib="casuser" casout="Toyotas" outcaslib="casuser" replace;
    contents casdata="ToyotaCars" incaslib="casuser";
    contents casdata="Toyotas" incaslib="casuser";
run;

proc casutil;
    droptable casdata="Toyotas" incaslib="casuser";
    droptable casdata="ToyotaCars" incaslib="casuser";
run;
