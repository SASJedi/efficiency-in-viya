%let path=/home/student/Courses/efficiency;
%let OracleServer=client.demo.sas.com:1521/ORCL;
options stimer sastrace=off;
cas cs sessopts=(caslib="casuser"
/*                 ,MESSAGELEVEL="ALL" */
/*                 ,METRICS=TRUE */
                ,timeout=14400);

proc cas;
table.addCaslib /
   caslib="db2CAS"
  ,datasource={srctype="oracle"
               username="STUDENT"
               password="Metadata0"
               path="//&OracleServer"}
;
table.addCaslib /
   caslib="SAS2CAS"
  ,path="&path/data"
;
run;
quit;

/* Traditional SAS datasets live here */
libname sas base "&path/data";
/* DBMS (Oracle) tables live here */
libname db oracle path="&OracleServer" user=student pw=Metadata0 schema=student;

/* Add librefs to caslibs using the CAS engine to use CAS tables in Compute Server */
libname db2CAS cas caslib="db2CAS";
libname SAS2CAS cas caslib="SAS2CAS";
libname casuser cas caslib="casuser";
