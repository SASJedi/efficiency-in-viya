/* Load Oracle tables to db2cas caslib */
proc cas;
   table.tableInfo result=ri/ caslib="db2cas";
run;
do thisName over {"WIDE","NARROW","NARROW2","BIG_ORDER_FACT"};
   if findtable(ri) then someData=TRUE;
    else someData=FALSE;
   if someData and dim(ri.tableInfo.where(name=thisName)[,1]) >0 then thisTable=ri.tableInfo.where(name=thisName)[,1][1];
    else thisTable="NotFound";
	if ! (thisTable=thisName) then do;
	table.loadTable /
	    caslib="db2cas"
	   ,path=thisName
	   ,casout={caslib="db2cas",name=thisName,replace=true}
	;
	end;
end;
run;
quit;