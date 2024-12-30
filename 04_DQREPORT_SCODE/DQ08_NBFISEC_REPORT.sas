/* ************************************************************************************************* */
/* STEP 0: Prepare Format to */
/* ************************************************************************************************* */
%macro gen_NBFISEC_DQReport(stdte=, rmp=);

	%let dtdte	= %SYSFUNC(intnx(MONTH,%SYSFUNC(inputn(&stdte.01,yymmdd8.)),0,END));
	%let stYMD	= %SYSFUNC(putn(&dtdte.,yymmdds10.));
	proc format;
		value NBFISEC_excl
		1 -< 2 = "1. EAD pre ccf = 0, blank or no info in CAR"
		2 -< 3 = "2. Rating expired at the report date (next_review_date <= &stYMD.)"
		4 -< 5 = "3. Already defaulted at the beginning of the reporting period (final rating = 8)"
		5 -< 6 = "4. Incomplete Financials"
		;
		value $NBFISEC_ds
		"&rmp.A_&stdte." = "A"
		"&rmp.B_&stdte." = "B"
		;
	run;
	/* *************************************************************************************************** */
	/* STEP 1 - Prepare DATA LIST */
	/* *************************************************************************************************** */
	data work.datalist;
		set sashelp.vcolumn;
		where libname="RAW_RMP" and memname contains("&rmp.") and memname contains "&stdte.";
		_index = put(strip(memname),$NBFISEC_ds.);
	run;
	data dummy;
		length label $255;
		_index="A"; label="PRE_CCF_EAD";varnum=999998;output;
		_index="A"; label="POST_CCF_EAD";varnum=999999;output;
	run;
	data work.datalist_v1; set work.datalist work.dummy; run;
	proc sort data=work.datalist_v1; by _index varnum; run;
	/* *************************************************************************************************** */
	/* STEP 2 - Prepare sources of A,B, etc  */
	/* *************************************************************************************************** */
	proc sort data=mart.&rmp._&stdte.(keep=n_raid n_archive_id sasflag_dq_excluded) out=work.&rmp._key_&stdte.;
		where not missing(sasflag_dq_excluded);
		by n_raid n_archive_id; 
	run;
	%macro src_RAID(srctype=);
		proc sort data=raw_rmp.&rmp.&srctype._&stdte. out=tmp1;	by n_raid n_archive_id; run;
		data work.&rmp.&srctype._&stdte.;
			merge tmp1(in=a) work.&rmp._key_&stdte.(in=b);
			by n_raid n_archive_id; 
			if a;
		run;
	%mend;
	%src_RAID(srctype=B);

	/* *************************************************************************************************** */
	/* STEP 3 - Prepare default data */
	/* *************************************************************************************************** */
	%let dtdte		= %SYSFUNC(intnx(MONTH,%SYSFUNC(inputn(&stdte.01,yymmdd8.)),0,END));
	%let dtdteb		= %SYSFUNC(intnx(MONTH,&dtdte.,-12,END));
	%let stdteb		= %SYSFUNC(putn(&dtdteb.,yymmn6.));
	%if %sysfunc(exist(mart.&rmp._ALL_&stdteb.)) %then %do;
		data work.default_NBFISEC_&stdteb.;
			set mart.&rmp._ALL_&stdteb.;
			where not missing(sasflag_ddm_ind);
		run;
	%end;
	/* *************************************************************************************************** */
	/* STEP 4 - Output files to EXCEL , PROC EXPORT cannot be used because of encoding issue */
	/* *************************************************************************************************** */
	options missing='';
	ods tagsets.ExcelXP file="&dir_rptdq./DQ_Data_NBFISEC_&stdte..xls" style=minimal;
	* Exclusion Summary *;
	ods tagsets.ExcelXP options(sheet_name='src_Excluded');
		/* Because proc freq cannot show ALL format label when count=0, we need using proc means instead
		proc freq data= mart.&rmp._key_&stdte. order=formatted;;
			table sasflag_dq_excluded/missing nopercent nocum; 
			format sasflag_dq_excluded NBFISEC_excl.;
		run;
		*/
	proc means data=mart.&rmp._&stdte.(where=(not missing(sasflag_dq_excluded))) n completetypes nonobs maxdec=0;
		class sasflag_dq_excluded/preloadfmt ;
		var n_raid  /*n_archive_id*/;
		format sasflag_dq_excluded NBFISEC_excl.;
	run;
	* Datalist *;
	ods tagsets.ExcelXP options(sheet_name='src_Datalist');
		proc print data=work.datalist_v1; var label _index; run;
	* Raw Source *;
	ods tagsets.ExcelXP options(sheet_name='src_A');
		proc print data=mart.&rmp._ALL_&stdte. noobs label; var _all_/ style (data) = {tagattr ="format:@"}; run;
	ods tagsets.ExcelXP options(sheet_name='src_B');
		proc print data=work.&rmp.B_&stdte. noobs label; var _all_/ style (data) = {tagattr ="format:@"}; run;
	* Default Data *;
	%if %sysfunc(exist(work.default_NBFISEC_&stdteb.)) %then %do;
	ods tagsets.ExcelXP options(sheet_name='Default');
		proc print data=work.default_NBFISEC_&stdteb. noobs label; run;
	%end;
	* Report Info *;
	ods tagsets.ExcelXP options(sheet_name='RptDate');
		data rptdte;date="&stYMD.";run;
		proc print data=rptdte noobs label; run;

	ods tagsets.ExcelXP close;
	options missing=.;
%mend;
%gen_NBFISEC_DQReport(stdte=&st_BackMth., 	rmp=RMP2BIONBFISEC);
%gen_NBFISEC_DQReport(stdte=&st_RptMth.,	rmp=RMP2BIONBFISEC);
