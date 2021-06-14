/* Loading data with PROC CASUTIL */
proc casutil;
    load casdata="toyotas.csv" incaslib="casuser"
         casout="Toyotas" outcaslib="casuser" replace;
    contents casdata="Toyotas" incaslib="casuser";
run;
cas cs listhistory 5;

/* Grab the loadTable and columnInfo action code from the log and paste here */
proc cas;
table.loadTable / path='toyotas.csv', caslib='CASUSER(student)', casOut={name='Toyotas', 
      caslib='CASUSER(student)', replace=true};
table.columnInfo / table={name='Toyotas', caslib='CASUSER(student)', singlePass=false}; 
quit;

cas cs listhistory 2;

/* Simplifying coding and program maintenance with CAS variables */
/* Using CAS variables for values that need to be repeated in code */
proc cas;
/* myTable={name='toyotas',caslib='CASUSER'}; */
myTable={name='cars',caslib='CASUSER'};
stats={name=myTable.name||"Summary",caslib="casuser"};

table.columnInfo result=rci/
     table=myTable
;
describe rci;
columns=rci.columnInfo.where(Column like "M%" and Type='double')[,"Column"];
simple.summary /
   table=myTable
  ,inputs=columns
  ,casout=stats||{replace=true}
;
table.fetch / 
   table=stats
;
quit;

/* Using CAS result tables for data-driven programming */
proc cas;
table.tableInfo result=rti / 
    caslib="casuser"
;
/* describe rti; */

tables=rti.tableInfo[,"Name"];
/* describe tables; */

do thisName over tables;
   thisTable={caslib="casuser", name=thisName};
   table.tableDetails / caslib=thisTable.caslib name=thistable.name;
   table.columnInfo / table=thisTable;
   table.fetch / table=thisTable to=5;
end;
quit;

/* Execute DATA step code directly in CAS */
proc cas;
source myDATAstep;
data casuser.ToyotaSummary;
   set casuser.toyotas;
   by DriveTrain;
   if first.drivetrain then do;
      call missing (city,hwy, count);
   end;
   city+MPG_City;
   hwy+MPG_Highway;
   count+1;
   if last.drivetrain then do;
      AvgMPG=round(sum(city,hwy)/count,.1);
      output;
   end;
   keep DriveTrain Count AvgMpg;
run;
endsource;

dataStep.runCode /
   code=myDATAstep
;

table.fetch / 
   table={caslib="casuser",name="ToyotaSummary"} 
;
quit;

proc cas;
/* Get rid of temporary tables using data-driven techniques */
table.tableInfo result=rti / 
    caslib="casuser"
;
tables=rti.tableInfo[,"Name"];
do thisName over tables;
   thisTable={caslib="casuser", name=thisName};
   table.dropTable / caslib=thisTable.caslib name=thistable.name;
end;

/* Get rid of data files isn a caslib's source */
table.fileInfo result=rfi / 
    caslib="casuser"
;
files=rfi.fileInfo[,"Name"];
do thisFile over files;
   thisFile={caslib="casuser", name=thisFile};
   table.deletesource / caslib=thisFile.caslib source=thisFile.name;
end;
quit;
