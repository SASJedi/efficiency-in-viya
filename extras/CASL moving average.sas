data casuser.test; 
   length Customer $5;
   do Customer="Able", "Baker";
	    do y=2020 to 2021;
	        do i=1 to 12;
	            date=mdy(i,1,y);
	            total=round(rand("uniform",1,10));
	            output;
	        end;
	    end;
    end;
    format date date9.;
    drop i y;
run;


proc cas;
    tblIN={name="test",caslib="casuser"};
    tblOut={name="summary",caslib="casuser"};
    table.fetch / table=tblIN, index=FALSE;
    aggregation.aggregate /
        table=tblIN|| {groupBy={"Customer","Date"}},
        varSpecs={
            {name="Total",subset="MEAN"}
        },
        ID="Date",
        Interval="MONTH",
        windowInt="QTR",
        casOut=tblOut||{replace=TRUE};
    table.fetch / table=tblin to=50, index=FALSE;
    table.fetch / table=tblOut to=50, index=FALSE;
quit;